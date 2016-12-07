#ifndef SBUS_H
#define SBUS_H

#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct sbus;
struct sbus_peak;
struct sbus_route;

#include "small/rlist.h"
#include <pthread.h>
#include <stdint.h>

#define SBUS_CHUNK_MUL	10
#define SBUS_CHUNK_SIZE (1 << SBUS_CHUNK_MUL)
#define SBUS_CHUNK_MASK (SBUS_CHUNK_SIZE - 1)

struct sbus_watcher
{
	void (*notify)(void *);
	void *notify_arg;
	struct rlist item;
};

struct sbus
{
	struct rlist peaks;
	pthread_mutex_t mutex;
	struct rlist watchers;
};

struct sbus_peak
{
	struct rlist item;
	struct sbus *sbus;
	char *name;
	void (*ready)(void *);
	void *ready_arg;

	struct sbus_route *route;
	int stail;
};


struct sbus_chunk;

struct sbus_route
{
	struct sbus_peak *peak;
	struct sbus_chunk *rchunk, *wchunk;
	uint64_t rpos;
	uint64_t wpos;
	unsigned priority;
	bool exiting;
	void (*exit)(void *);
	void *exit_arg;
	struct sbus_route *next;
} __attribute__((aligned(64)));


struct sbus_chunk
{
	void *messages[SBUS_CHUNK_SIZE];
	struct sbus_chunk *next;
};


struct sbus *
sbus_create();

int
sbus_free(struct sbus *sbus, void (*notify)(void *), void *notify_arg);

/* Creates output with name for accepting ffcalls
 * ready_cb will be called if one of inputs has shanged his state to not empty */
struct sbus_peak *
sbus_attach(struct sbus *sbus, char *name,
	void (*ready)(void *), void *ready_arg,
	void (*notify)(void *arg), void *notify_arg);

/* Disconnect peak from sbus. Will block until all routes will be disconnected */
int
sbus_detach(struct sbus_peak *peak,
	    void (*notify)(void *arg), void *notify_arg);

/* Create route (input) to peak with name.
 * Will block until peak will be accessible */
struct sbus_route *
sbus_route(struct sbus *sbus, char *name, unsigned priority,
	   void (*notify)(void *arg), void *notify_arg);

/* Disconnect route from peak. If route is not empty returns error
 * and call unroute cb alfter flushing route.
 * Route won't accept any new requests */
int
sbus_unroute(struct sbus_route *route,
	     void (*notify)(void *arg), void *notify_arg);

/* Get request from peak. If all routes is empty return NULL */
void *
sbus_get(struct sbus_peak *peak);
unsigned int
sbus_get_many(struct sbus_peak *peak, void **data, unsigned int n);

/* Put request to peak throught route. Returns error
 * if route should be disconnected */
int
sbus_put(struct sbus_route *route, void *msg);

int
sbus_put_start(struct sbus_route *route, void *msg);

int
sbus_put_done(struct sbus_route *route);

#if defined(__cplusplus)
}
#endif /* defined(__cplusplus) */

#endif
