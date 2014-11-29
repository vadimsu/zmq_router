#include <specific_includes/dummies.h>
#include <specific_includes/linux/types.h>
#include <specific_includes/linux/bitops.h>
#include <specific_includes/linux/slab.h>
#include <specific_includes/linux/hash.h>
#include <specific_includes/linux/socket.h>
#include <api.h>

void app_main_loop();
struct socket *client_socks[10];
void app_init(char *my_ip_addr,unsigned short my_port,char *peer_ip_addr,unsigned short peerport)
{
	create_server_socket(my_ip_addr,my_port);
        client_socks[0] = create_client_socket(my_ip_addr,0,peer_ip_addr,peerport);
	app_main_loop();
}
