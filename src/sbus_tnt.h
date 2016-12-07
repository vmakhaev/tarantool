#ifndef SBUS_TNT_H
#define SBUS_TNT_H

#include "sbus.h"
#include "fiber.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

extern struct sbus *sbus;

struct sbus_peak *
sbus_attach_lock(struct sbus *sbus, char *name,
		 void (*ready)(void *), void *ready_arg);

int
sbus_detach_lock(struct sbus_peak *peak);

struct sbus_route *
sbus_route_lock(struct sbus *sbus, char *name, int priority);

int
sbus_unroute_lock(struct sbus_route *route);

int
sbus_free_lock(struct sbus *sbus);

struct sbus_wake_ctx {
	struct ev_loop *loop;
	struct ev_async async;
};

void
sbus_wake_cb(void *);

void
sbus_wake_init(struct sbus_wake_ctx *wake_ctx, struct fiber *fiber);

struct sbus_pool;

struct sbus_pool *
sbus_attach_pool(struct sbus *sbus, char *name, unsigned pool_size,
		  uint32_t pool_batch);

int
sbus_detach_pool(struct sbus_pool *pool);

#if defined(__cplusplus)
}
#endif /* defined(__cplusplus) */

#endif
