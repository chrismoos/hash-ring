#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>
#include "erl_driver.h"

#if ERL_DRV_EXTENDED_MAJOR_VERSION < 2
typedef int ErlDrvSizeT;
typedef int ErlDrvSSizeT;
#endif

#include "hash_ring.h"

/**
 * When the driver starts it is intialized with space for INITIAL_RING_SPACE rings.
 * If more than INITIAL_RING_SPACE rings are created, the driver will make the space larger.
 */
#define INITIAL_RING_SPACE 1024

// Commands
#define COMMAND_CREATE_RING     0x01
#define COMMAND_DELETE_RING     0x02
#define COMMAND_ADD_NODE        0x03
#define COMMAND_REMOVE_NODE     0x04
#define COMMAND_FIND_NODE       0x05
#define COMMAND_SET_MODE        0x06

#define RETURN_OK 0x00
#define RETURN_ERR 0x01

typedef struct {
    ErlDrvPort port;
    
    int numRings;
    hash_ring_t **rings;
    
    // Marks rings as used/unused
    unsigned char *ring_usage;
} hash_ring_data;

static ErlDrvData hash_ring_drv_start(ErlDrvPort port, char *buff)
{
    hash_ring_data* d = (hash_ring_data*)driver_alloc(sizeof(hash_ring_data));
    d->port = port;
    
    d->numRings = INITIAL_RING_SPACE;
    d->rings = (hash_ring_t**)driver_alloc(sizeof(hash_ring_t) * d->numRings);
    
    d->ring_usage = (unsigned char*)driver_alloc(sizeof(unsigned char) * d->numRings);
    memset(d->ring_usage, 0, sizeof(unsigned char) * d->numRings);
    
    return (ErlDrvData)d;
}
static void hash_ring_drv_stop(ErlDrvData handle)
{
    hash_ring_data *d = (hash_ring_data*)handle;
    
    int x;
    for(x = 0; x < d->numRings; x++) {
        if(d->ring_usage[x] == 1) {
            hash_ring_free(d->rings[x]);
        }
    }
    
    driver_free(d->rings);
    driver_free(d->ring_usage);

    driver_free((char*)handle);
}

static int find_open_ring_space(hash_ring_data *data) {
    int x;
    for(x = 0; x < data->numRings; x++) {
        // spot found in the ring
        if(data->ring_usage[x] == 0) {
            return x;
        }
    }
    
    void *resized;
    // no space found, enlarge the ring space
    resized = driver_realloc(data->ring_usage, sizeof(unsigned char) * (data->numRings + INITIAL_RING_SPACE));
    if(resized == NULL) {
        return -1;
    }
    data->ring_usage = (unsigned char*)resized;
    memset(data->ring_usage + data->numRings, 0, INITIAL_RING_SPACE);
    
    resized = (hash_ring_t**)driver_realloc(data->rings, sizeof(hash_ring_t*) * (data->numRings + INITIAL_RING_SPACE));
    if(resized == NULL) {
        return -1;
    }
    data->rings = (hash_ring_t**)resized;
    
    data->numRings += INITIAL_RING_SPACE;
    return (data->numRings - INITIAL_RING_SPACE);
}

static uint32_t readUint32(uint8_t *buf) {
    uint32_t num;
    memcpy(&num, buf, 4);
    return ntohl(num);
}

static void hash_ring_drv_output(ErlDrvData handle, char *buff, ErlDrvSizeT bufflen)
{
    hash_ring_data* d = (hash_ring_data*)handle;
    char res = RETURN_ERR;
    
    // Check the command
    if(bufflen == 6 && buff[0] == COMMAND_CREATE_RING) {
        uint32_t numReplicas;
        memcpy(&numReplicas, &buff[1], 4);
        numReplicas = ntohl(numReplicas);

        int index = find_open_ring_space(d);
        if(index != -1) {
            d->ring_usage[index] = 1;
            
            d->rings[index] = hash_ring_create(numReplicas, buff[5]);
            
            index = htonl(index);
            driver_output(d->port, (char*)&index, 4);
            return;
        }
    }
    else if(bufflen == 5 && buff[0] == COMMAND_DELETE_RING) {
        uint32_t index;
        memcpy(&index, &buff[1], 4);
        index = ntohl(index);

        if(d->numRings > index) {
            if(d->ring_usage[index] == 1) {
                d->ring_usage[index] = 0;
                hash_ring_free(d->rings[index]);
                res = RETURN_OK;
            }
        }
    }
    else if(bufflen >= 9 && buff[0] == COMMAND_ADD_NODE) {
        uint32_t index = readUint32((unsigned char*)&buff[1]);
        uint32_t nodeLen = readUint32((unsigned char*)&buff[5]);
        if((bufflen - 9) == nodeLen && d->numRings > index && d->ring_usage[index] == 1) {
            hash_ring_add_node(d->rings[index], (unsigned char*)&buff[9], nodeLen);
            res = RETURN_OK;
        }
    }
    else if(bufflen >= 9 && buff[0] == COMMAND_REMOVE_NODE) {
        uint32_t index = readUint32((unsigned char*)&buff[1]);
        uint32_t nodeLen = readUint32((unsigned char*)&buff[5]);
        if((bufflen - 9) == nodeLen && d->numRings > index && d->ring_usage[index] == 1) {
            hash_ring_remove_node(d->rings[index], (unsigned char*)&buff[9], nodeLen);
            res = RETURN_OK;
        }
    }
    else if(bufflen >= 9 && buff[0] == COMMAND_FIND_NODE) {
        uint32_t index = readUint32((unsigned char*)&buff[1]);
        uint32_t keyLen = readUint32((unsigned char*)&buff[5]);
        if((bufflen - 9) == keyLen && d->numRings > index && d->ring_usage[index] == 1) {
            hash_ring_node_t *node = hash_ring_find_node(d->rings[index], (unsigned char*)&buff[9], keyLen);
            if(node != NULL) {
                driver_output(d->port, (char*)node->name, node->nameLen);
                return;
            }
        }
    }
    else if(bufflen == 6 && buff[0] == COMMAND_SET_MODE) {
        uint32_t index = readUint32((unsigned char*)&buff[1]);
        uint8_t mode = (uint8_t)buff[5];
        if(d->numRings > index && d->ring_usage[index] == 1) {
            if(hash_ring_set_mode(d->rings[index], mode) == HASH_RING_OK) {
               res = RETURN_OK;
            }
        }
    }
    
    // default return
    driver_output(d->port, &res, 1);
}

ErlDrvEntry hash_ring_driver_entry = {
  NULL, /* init */
  hash_ring_drv_start,
  hash_ring_drv_stop,
  hash_ring_drv_output,
  NULL, /* ready_input */
  NULL, /* ready_output */
  "hash_ring_drv", /* driver_name */
  NULL, /* finish */
  NULL, /* handle (reserved) */
  NULL, /* control */
  NULL, /* timeout */
  NULL, /* outputv */
  NULL, /* ready_async */
  NULL, /* flush */
  NULL, /* call */
  NULL, /* event */
  ERL_DRV_EXTENDED_MARKER,
  ERL_DRV_EXTENDED_MAJOR_VERSION,
  ERL_DRV_EXTENDED_MINOR_VERSION,
  0, /* driver_flags */
  NULL, /* handle2 (reserved) */
  NULL, /* process_exit */
  NULL /* stop_select */
};

DRIVER_INIT(hash_ring_drv) /* must match name in driver_entry */
{
    return &hash_ring_driver_entry;
}
