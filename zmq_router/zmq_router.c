#include <specific_includes/dummies.h>
#include <specific_includes/linux/types.h>
#include <specific_includes/linux/bitops.h>
#include <specific_includes/linux/slab.h>
#include <specific_includes/linux/hash.h>
#include <specific_includes/linux/socket.h>
#include <specific_includes/linux/sockios.h>
#include <specific_includes/linux/if_ether.h>
#include <specific_includes/linux/netdevice.h>
#include <specific_includes/linux/etherdevice.h>
#include <specific_includes/linux/ethtool.h>
#include <specific_includes/linux/skbuff.h>
#include <specific_includes/net/net_namespace.h>
#include <specific_includes/net/sock.h>
#include <specific_includes/linux/rtnetlink.h>
#include <specific_includes/net/dst.h>
#include <specific_includes/net/checksum.h>
#include <specific_includes/linux/err.h>
#include <specific_includes/linux/if_arp.h>
#include <specific_includes/linux/if_vlan.h>
#include <specific_includes/linux/ip.h>
#include <specific_includes/net/ip.h>
#include <specific_includes/linux/ipv6.h>
#include <specific_includes/linux/in.h>
#include <string.h>
#include <rte_config.h>
#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_timer.h>
#include <api.h>
#include <porting/libinit.h>


uint64_t user_on_state_machine_run = 0;
uint64_t user_on_tx_opportunity_called = 0;
uint64_t user_on_get_tx_buf_failed = 0;
uint64_t user_queued_buf_is_too_big = 0;
uint64_t user_on_tx_opportunity_getbuff_called = 0;
uint64_t user_on_handshake = 0;
uint64_t user_on_length = 0;
uint64_t user_on_get_length = 0;
uint64_t user_on_payload = 0;
uint64_t user_on_tx_opportunity_api_failed = 0;
uint64_t user_on_tx_opportunity_api_mbufs_sent = 0;
uint64_t user_on_length_mismatch = 0;
uint64_t user_on_rx_opportunity_called = 0;
uint64_t user_on_rx_opportunity_called_wo_result = 0;

enum
{
    NULL_TYPE,
    SERVER_PIPE_TYPE,
    CLIENT_PIPE_TYPE
};

enum
{
    HANDSHAKE_STATE,
    LENGTH_STATE,
    PAYLOAD_STATE
};
#define TRACE_ZMQ_ROUTER_ENABLED 0
#if TRACE_ZMQ_ROUTER_ENABLED
#define TRACE_ZMQ_ROUTER(s) printf("%s %d %s\n",__func__,__LINE__,s);
#else
#define TRACE_ZMQ_ROUTER(s)
#endif

struct rte_mbuf_queue
{
    struct rte_mbuf *head;
    struct rte_mbuf *tail;
};

static inline void move_queue(struct rte_mbuf_queue *srcqueue,struct rte_mbuf_queue *dstqueue)
{
    if(dstqueue->head == NULL) {
        dstqueue->head = srcqueue->head;
        dstqueue->tail = srcqueue->tail;
    }
    else {
        dstqueue->tail->pkt.next = srcqueue->head;
        dstqueue->head->pkt.pkt_len += srcqueue->head->pkt.pkt_len;
    }
    srcqueue->head = NULL;
    srcqueue->tail = NULL;
}

static inline struct rte_mbuf *dequeue_mbuf_till_length(struct rte_mbuf_queue *queue,
                                                         int *len)
{
    struct rte_mbuf *first = NULL,*curr,*tail = NULL;
    int original_pkt_len = 0;
    curr = queue->head;
    if(curr)
        original_pkt_len = curr->pkt.pkt_len;
    if(original_pkt_len <= *len) {
        first = queue->head;
        queue->head = NULL;
        queue->tail = NULL;
        *len -= original_pkt_len;
        user_on_tx_opportunity_api_mbufs_sent++;
        return first;
    }
    while((curr)&&(*len > 0)) {
        if(curr->pkt.data_len <= *len) {
            if(first == NULL) {
                first = curr;
                tail = curr; 
                first->pkt.pkt_len = curr->pkt.data_len;
            }
            else {
                tail->pkt.next = curr;
                tail = tail->pkt.next;
                first->pkt.pkt_len += curr->pkt.data_len;
            }
            user_on_tx_opportunity_api_mbufs_sent++;
            original_pkt_len -= curr->pkt.data_len;
            *len -= curr->pkt.data_len;
            curr = curr->pkt.next;
            if(curr)
                curr->pkt.pkt_len = original_pkt_len;
        }
        else
           break;
    }
    return first;
}

static inline void substitute_data(struct rte_mbuf_queue *queue,char *data,int offset, int len)
{
    struct rte_mbuf *curr;
    int idx = 0,copied = 0,offset2 = 0;
    curr = queue->head;
    
    while((curr)&&(copied < len)) {
        if(idx == offset) {
            memcpy(&((char *)curr->pkt.data)[offset2],
                   &data[copied],
                   (curr->pkt.data_len > (len - copied)) ?
                     (len - copied): curr->pkt.data_len);
            copied += (curr->pkt.data_len > (len - copied)) ? (len - copied) : curr->pkt.data_len; 
            curr = curr->pkt.next;
        }
        else {
            if((offset - idx) > curr->pkt.data_len) {
                idx += curr->pkt.data_len;
                curr = curr->pkt.next;
                offset2 = 0;
            }
            else {
                offset2 = offset - idx;
                idx = offset;
            }
        }
    }
}

static inline void shrink_data(struct rte_mbuf_queue *queue)
{
    if(queue->tail) {
        if(queue->tail->pkt.data_len < 10) {
            printf("%s %d \n",__FILE__,__LINE__);
        }
        else
            queue->tail->pkt.data_len -= 10;
    }
}

static inline void enqueue_mbuf(struct rte_mbuf_queue *queue,struct rte_mbuf *mbuf)
{
    if(!queue->head) {
        queue->head = mbuf;
        queue->tail = mbuf;
        mbuf->pkt.pkt_len = mbuf->pkt.data_len;
    }
    else {
        queue->tail->pkt.next = mbuf;
        queue->tail = queue->tail->pkt.next;
        queue->head->pkt.pkt_len += mbuf->pkt.data_len;
    }
}

static inline int check_if_enqueued_and_enqueue_mbuf(struct rte_mbuf_queue *queue,struct rte_mbuf *mbuf)
{
    if(queue->tail == mbuf)
        return 0;
    enqueue_mbuf(queue,mbuf);
    return 1;
}

static inline int queue_empty(struct rte_mbuf_queue *queue)
{
    return (queue->head == NULL);
}

static inline int get_queue_length(struct rte_mbuf_queue *queue)
{
    return (queue->head) ? queue->head->pkt.pkt_len : 0;
}

static inline void print_mbuf(struct rte_mbuf *mbuf)
{
    int i;
    unsigned char *pp = mbuf->pkt.data;

    TRACE_ZMQ_ROUTER(pp+4);
#if TRACE_ZMQ_ROUTER_ENABLED
    for(i = 0;i < mbuf->pkt.data_len;i++)
       printf(" %x ",pp[i]);
#endif
}

typedef struct zmq_peer
{
    unsigned int type;
    struct rte_mbuf_queue messages;
    struct rte_mbuf_queue response;
    struct socket *peer_sock;
    int    state;
    unsigned long bytes_expected;
}__attribute__((packed))zmq_peer_t;

zmq_peer_t outgoing_peers[10];

struct kmem_cache *zmq_peers = NULL; 

#ifdef OPTIMIZE_SENDPAGES
/* this is called from tcp_sendpages when tcp knows exactly
 * how much data to  send. copy contains a max buffer size,
 * it must be updated by the callee
 * A user can also set a socket's user private data
 * for its applicative needs
 */
struct rte_mbuf *user_get_buffer(struct sock *sk,int *copy)
{
	struct rte_mbuf *first = NULL;
        zmq_peer_t *peer;
	user_on_tx_opportunity_getbuff_called++;

        peer = app_glue_get_user_data(sk->sk_socket);
        if(!peer) {
            *copy = 0;
            TRACE_ZMQ_ROUTER("cannot get peer");
            return NULL;
        }
        first = dequeue_mbuf_till_length(&peer->response,copy);
        if(first != NULL) { 
            return first;
        }
#if 1
        //TRACE_ZMQ_ROUTER("no mbufs or mbufs are too big");
        first = dequeue_mbuf_till_length(&peer->messages,copy);
        if(first) {
            user_on_tx_opportunity_api_mbufs_sent++;
        }
	return first;
#else
        user_on_get_tx_buf_failed++;
        return NULL;
#endif
}
#endif
int user_on_transmission_opportunity(struct socket *sock)
{
	struct page page;
	int i = 0,sent = 0;
	uint32_t to_send_this_time;
	uint64_t ts = rte_rdtsc();
        zmq_peer_t *peer = app_glue_get_user_data(sock);
        if(!peer) {
            return 0;
        }
        if(get_queue_length(&peer->response) == 0) {
            return 0;
        }
	user_on_tx_opportunity_called++;

	while(likely((to_send_this_time = app_glue_calc_size_of_data_to_send(sock)) > 0)) {
		sock->sk->sk_route_caps |= NETIF_F_SG | NETIF_F_ALL_CSUM;
		i = kernel_sendpage(sock, &page, 0,/*offset*/to_send_this_time /* size*/, 0 /*flags*/);
		if(i <= 0) {
			user_on_tx_opportunity_api_failed++;
                        break;
                }
                else
                    sent += i;
                if(get_queue_length(&peer->response) == 0) {
                    break;
                }
	}
	return sent;
}

int get_zmq_message_length(zmq_peer_t *peer)
{
    unsigned long length,bytes_copied = 0,bytes_to_copy,target_size,offset = 2;
    unsigned char *dst = (unsigned char *)&peer->bytes_expected;
    struct rte_mbuf *mbuf = peer->messages.head;
    unsigned char *src = (unsigned char *)mbuf->pkt.data;
    user_on_get_length++; 
    if(mbuf->pkt.pkt_len <= offset) {
       return -1;
    }
    while((mbuf)&&(offset)) {
        if(mbuf->pkt.data_len > offset) {
            src += offset;
            if(*src == 0xFF) {
                TRACE_ZMQ_ROUTER("FF");
                if((mbuf->pkt.data_len - offset) > 0) {
                    TRACE_ZMQ_ROUTER("data_len > offset");
                    src++;
                    if((mbuf->pkt.data_len - offset) >= sizeof(unsigned long)) {
                        TRACE_ZMQ_ROUTER("enough bytes received to retrieve length");
                        bcopy(src,dst,sizeof(unsigned long));
                        printf("%s %d %d\n",__func__,__LINE__,peer->bytes_expected);
                        return 1;
                    }
                    else {
                        TRACE_ZMQ_ROUTER("not enough bytes received to retrieve length");
                        bcopy(src,dst,sizeof(unsigned long) - (mbuf->pkt.data_len - offset));
                        bytes_copied = sizeof(unsigned long) - (mbuf->pkt.data_len - offset);
                        dst += bytes_copied;
                    }
                }
                mbuf = mbuf->pkt.next;
                src = mbuf->pkt.data;
                break;
            }
            TRACE_ZMQ_ROUTER("expected bytes recalculated");
            peer->bytes_expected = *src;
            *src = (*src) - 10;
            return 1; 
        }
        offset -= mbuf->pkt.data_len; 
        mbuf = mbuf->pkt.next; 
        src = mbuf->pkt.data;
    }
    
    while((bytes_copied < sizeof(unsigned long))&&
          (mbuf)) {
        TRACE_ZMQ_ROUTER("");
        if(mbuf->pkt.data_len > (sizeof(unsigned long) - bytes_copied)) {
            TRACE_ZMQ_ROUTER("");
            bytes_to_copy = (sizeof(unsigned long) - bytes_copied);
        }
        else {
            TRACE_ZMQ_ROUTER("");
            bytes_to_copy = (sizeof(unsigned long) - bytes_copied) - mbuf->pkt.data_len;
        }
        bcopy(src,dst,bytes_to_copy);
        bytes_copied += bytes_to_copy;
        dst += bytes_to_copy;
        mbuf = mbuf->pkt.next;
        src = mbuf->pkt.data;
    }
    return (bytes_copied == sizeof(unsigned long));
}
int parse_zmq_length(zmq_peer_t *peer,struct rte_mbuf *mbuf)
{
    unsigned char *p;
    user_on_length++;
    enqueue_mbuf(&peer->messages,mbuf);
    
    if(peer->messages.head->pkt.pkt_len < 3) {
        TRACE_ZMQ_ROUTER("pkt_len < 3");
        return 1;
    }
    p = (unsigned char *)peer->messages.head->pkt.data;
    if(p[0] != 0x1) {
        TRACE_ZMQ_ROUTER("p[0]!=0x1");
        return 1;
    }
    if(get_zmq_message_length(peer)) {
        TRACE_ZMQ_ROUTER("");
        peer->bytes_expected += 3; /* preamble + flags + length */
        peer->state = PAYLOAD_STATE;
        return 0;
    }
    TRACE_ZMQ_ROUTER("");
    return 1;
}
int parse_zmq_payload(zmq_peer_t *peer,struct rte_mbuf *mbuf)
{
    zmq_peer_t *other_peer;
    int rc;
    TRACE_ZMQ_ROUTER("");
    user_on_payload++;
    if(queue_empty(&peer->messages)) {
        TRACE_ZMQ_ROUTER("");
        return 1;
    }
    rc = check_if_enqueued_and_enqueue_mbuf(&peer->messages,mbuf);
    
    if(peer->messages.head->pkt.pkt_len > peer->bytes_expected) {
        TRACE_ZMQ_ROUTER("");
        user_on_length_mismatch++;
        return 1;
    }
    if(get_queue_length(&peer->messages) != peer->bytes_expected) {
        TRACE_ZMQ_ROUTER("");
        user_on_length_mismatch++;
        return 1;
    }
    if(!peer->peer_sock) {
        TRACE_ZMQ_ROUTER("");
        return 1;
    }
#if 0
    other_peer = app_glue_get_user_data(peer->peer_sock);
    if(!other_peer) {
        TRACE_ZMQ_ROUTER("");
        return 1;
    }
   
    if(queue_empty(&other_peer->messages)) {
        struct rte_mbuf *resp = app_glue_get_buffer();

        print_mbuf(mbuf);
        
        if(resp) {
            char *p_resp = (char *)resp->pkt.data;
            strcpy(p_resp,"FUCK YOU");
            resp->pkt.data_len = strlen("FUCK YOU");
            enqueue_mbuf(&peer->response,resp); 
        }
    }
    move_queue(&other_peer->messages,&peer->messages);
#endif
#if 0
    {
        char *p = (char *)mbuf->pkt.data + 4;
        p[0] = 'R';
        p[1] = 'E';
        p[2] = 'S';
        p[3] = 'P';
        p[4] = 'O';
        p[5] = 'N';
        p[6] = 'S';
        p[7] = 'E';
    }

    TRACE_ZMQ_ROUTER((char *)mbuf->pkt.data);
#else
    //substitute_data(&peer->messages,"RESPONSE",4,strlen("RESPONSE")); 
    move_queue(&peer->messages,&peer->response);
    shrink_data(&peer->response);
#endif
    peer->state = LENGTH_STATE;
    user_on_transmission_opportunity(peer->peer_sock);
    return 1;
}
void run_server_sock_state_machine(struct socket *sock,zmq_peer_t *peer,struct rte_mbuf *mbuf)
{ 
    unsigned char *p = mbuf->pkt.data;
    switch(peer->state) {
        case HANDSHAKE_STATE: /* handshake */
            print_mbuf(mbuf);
            if((p[0] == 0x1)&&(p[1] == 0x0)) {
                TRACE_ZMQ_ROUTER("handshake complete");
                enqueue_mbuf(&peer->response,mbuf);
                peer->state = LENGTH_STATE;
            }
            else {
                rte_pktmbuf_free_seg(mbuf);
            }
            break;
        case LENGTH_STATE:
            if(parse_zmq_length(peer,mbuf))
                break;
        case PAYLOAD_STATE:
            parse_zmq_payload(peer,mbuf);
            break;    	
    }
}
void run_client_sock_state_machine(zmq_peer_t *peer,struct rte_mbuf *mbuf)
{
    TRACE_ZMQ_ROUTER("");
}
void run_state_machine(struct socket *sock,zmq_peer_t *peer,struct rte_mbuf *mbuf)
{
    user_on_state_machine_run++;
    switch(peer->type){
        case CLIENT_PIPE_TYPE:
            run_client_sock_state_machine(peer,mbuf);
            break;
        case SERVER_PIPE_TYPE:
            run_server_sock_state_machine(sock,peer,mbuf);
            break;
    }
}
void user_data_available_cbk(struct socket *sock)
{
	struct msghdr msg;
	struct iovec vec;
	struct rte_mbuf *mbuf;
	int i,dummy = 1,idx;
        zmq_peer_t *peer = app_glue_get_user_data(sock);
        if(!peer) {
            return;
        }
	user_on_rx_opportunity_called++;
	memset(&vec,0,sizeof(vec));
	if(unlikely(sock == NULL)) {
		return;
	}
	while(unlikely((i = kernel_recvmsg(sock, &msg,&vec, 1 /*num*/, 1448 /*size*/, 0 /*flags*/)) > 0)) {
		dummy = 0;
                while((mbuf = msg.msg_iov->head) != NULL) {
                    msg.msg_iov->head = msg.msg_iov->head->pkt.next;
                    run_state_machine(sock,peer,mbuf);
//                    enqueue_mbuf(&peer->response,mbuf);
                    user_on_transmission_opportunity(sock);
                }
		memset(&vec,0,sizeof(vec));
	}
	if(dummy) {
		user_on_rx_opportunity_called_wo_result++;
	}
}
void user_on_socket_fatal(struct socket *sock)
{
	user_data_available_cbk(sock);/* flush data */
}
extern struct socket *client_socks[10];

int user_on_accept(struct socket *sock)
{
	struct socket *newsock = NULL;
        zmq_peer_t *peer;
	while(likely(kernel_accept(sock, &newsock, 0) == 0)) {
		newsock->sk->sk_route_caps |= NETIF_F_SG |NETIF_F_ALL_CSUM;
                peer = kmem_cache_alloc_node(zmq_peers, 0, rte_socket_id());
                if(peer) {
                    TRACE_ZMQ_ROUTER("new peer");
                    peer->messages.head = NULL;
                    peer->messages.tail = NULL;
                    peer->response.head = NULL;
                    peer->response.tail = NULL;
                    peer->bytes_expected = 2;
		    peer->peer_sock = /*client_socks[0]*/newsock;
                    peer->state = 0;
                    peer->type = SERVER_PIPE_TYPE;
                }
                app_glue_set_user_data(newsock,peer);
                user_data_available_cbk(newsock);
	}
}

void app_main_loop()
{
    uint8_t ports_to_poll[1] = { 0 };
	int drv_poll_interval = get_max_drv_poll_interval_in_micros(0);
	app_glue_init_poll_intervals(/*drv_poll_interval/(2*MAX_PKT_BURST)*/0,
	                             1000 /*timer_poll_interval*/,
	                             /*drv_poll_interval/(10*MAX_PKT_BURST)*/0,
	                             /*drv_poll_interval/(60*MAX_PKT_BURST)*/0);

        zmq_peers = kmem_cache_create("zmq_peers", sizeof(zmq_peer_t),100,0,NULL);

        memset(&outgoing_peers[0],0,sizeof(outgoing_peers[0]));
        outgoing_peers[0].type = CLIENT_PIPE_TYPE;
        app_glue_set_user_data(client_socks[0],&outgoing_peers[0]);
	while(1) {
		app_glue_periodic(1,ports_to_poll,1);
	}
}
/*this is called in non-data-path thread */
void print_user_stats()
{
#if 0
	printf("user_on_tx_opportunity_called %"PRIu64"\n",user_on_tx_opportunity_called);
	printf("user_on_handshake %"PRIu64" user_on_length %"PRIu64" user_on_get_length %"PRIu64" user_on_payload %"PRIu64" \n",
                user_on_handshake,user_on_length,user_on_get_length,user_on_payload);
	printf("user_on_length_mismatch %"PRIu64"\n",user_on_length_mismatch);
	printf("user_on_tx_opportunity_getbuff_called %"PRIu64"\n",user_on_tx_opportunity_getbuff_called);
	printf("user_on_tx_opportunity_api_failed %"PRIu64"\n",	user_on_tx_opportunity_api_failed);
	printf("user_on_rx_opportunity_called %"PRIu64"\n",user_on_rx_opportunity_called);
	printf("user_on_rx_opportunity_called_wo_result %"PRIu64"\n",user_on_rx_opportunity_called_wo_result);
        printf("user_on_tx_opportunity_api_mbufs_sent %"PRIu64"\n",user_on_tx_opportunity_api_mbufs_sent);
        printf("user_on_state_machine_run %"PRIu64"\n",user_on_state_machine_run);
        printf("user_on_get_tx_buf_failed %"PRIu64"\n",user_on_get_tx_buf_failed);
        printf("user_queued_buf_is_too_big %"PRIu64"\n",user_queued_buf_is_too_big);
#endif
}
