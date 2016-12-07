#include "sbus_tnt.h"

#include "fiber.h"

struct mutex_cond {
	pthread_mutex_t m;
	pthread_cond_t c;
};

static void
wake(struct mutex_cond *mc)
{
	pthread_mutex_lock(&mc->m);
	pthread_cond_signal(&mc->c);
	pthread_mutex_unlock(&mc->m);
}

struct sbus_peak *
sbus_attach_lock(struct sbus *sbus, char *name,
		 void (*ready)(void *), void *ready_arg)
{
	struct sbus_peak *peak;
	struct mutex_cond mc;
	pthread_mutex_init(&mc.m, NULL);
	pthread_cond_init(&mc.c, NULL);
	pthread_mutex_lock(&mc.m);
	while (!(peak = sbus_attach(sbus, name, ready, ready_arg,
				    (void (*)(void *))wake, &mc)))
		pthread_cond_wait(&mc.c, &mc.m);

	pthread_mutex_unlock(&mc.m);
	pthread_mutex_destroy(&mc.m);
	pthread_cond_destroy(&mc.c);
	return peak;
}

int
sbus_detach_lock(struct sbus_peak *peak)
{
	struct mutex_cond mc;
	pthread_mutex_init(&mc.m, NULL);
	pthread_cond_init(&mc.c, NULL);
	pthread_mutex_lock(&mc.m);
	while (sbus_detach(peak, (void (*)(void *))wake, &mc))
		pthread_cond_wait(&mc.c, &mc.m);

	pthread_mutex_unlock(&mc.m);
	pthread_mutex_destroy(&mc.m);
	pthread_cond_destroy(&mc.c);
	return 0;
}

struct sbus_route *
sbus_route_lock(struct sbus *sbus, char *name, int priority)
{
	struct sbus_route *route;
	struct mutex_cond mc;
	pthread_mutex_init(&mc.m, NULL);
	pthread_cond_init(&mc.c, NULL);
	pthread_mutex_lock(&mc.m);
	while (!(route = sbus_route(sbus, name, priority,
				    (void (*)(void *))wake, &mc)))
		pthread_cond_wait(&mc.c, &mc.m);

	pthread_mutex_unlock(&mc.m);
	pthread_mutex_destroy(&mc.m);
	pthread_cond_destroy(&mc.c);
	return route;
}

int sbus_unroute_lock(struct sbus_route *route)
{
	struct mutex_cond mc;
	pthread_mutex_init(&mc.m, NULL);
	pthread_cond_init(&mc.c, NULL);
	pthread_mutex_lock(&mc.m);
	while (sbus_unroute(route, (void (*)(void *))wake, &mc))
		pthread_cond_wait(&mc.c, &mc.m);

	pthread_mutex_unlock(&mc.m);
	pthread_mutex_destroy(&mc.m);
	pthread_cond_destroy(&mc.c);
	return 0;
}

int sbus_free_lock(struct sbus *sbus)
{
	struct mutex_cond mc;
	pthread_mutex_init(&mc.m, NULL);
	pthread_cond_init(&mc.c, NULL);
	pthread_mutex_lock(&mc.m);
	while (sbus_free(sbus, (void (*)(void *))wake, &mc))
		pthread_cond_wait(&mc.c, &mc.m);

	pthread_mutex_unlock(&mc.m);
	pthread_mutex_destroy(&mc.m);
	pthread_cond_destroy(&mc.c);
	return 0;
}

struct sbus_pool_call {
	void (*call)(void *);
	void *call_arg;
};

struct sbus_pool {
	bool done;
	unsigned size;
	unsigned used;
	struct fiber *sched;
	struct sbus_peak *peak;
	struct rlist idle;
	bool stail;
	uint32_t round_size;
	uint32_t pool_batch;
	struct sbus_wake_ctx wake;
};

static int
sbus_fiber_pool_f(va_list ap)
{
	struct sbus_pool *pool = va_arg(ap, struct sbus_pool *);
	struct sbus_pool_call *call;
	while (!pool->done) {
		rlist_del(&fiber()->state);
		while ((call = sbus_get(pool->peak))) {
			call->call(call->call_arg);
			if (++pool->round_size % pool->pool_batch == 0)
				break;
		}

		rlist_add_entry(&pool->idle, fiber(), state);
		if (!call) {
			pool->stail = true;
			fiber_yield();
		} else
			fiber_reschedule();
		//TODO: add timeout
	}
	--pool->used;
	return 0;
}

static int
fiber_sched_f(va_list ap)
{
	struct sbus_pool *pool = va_arg(ap, struct sbus_pool *);
	while (!pool->done) {
		pool->stail = 0;
		pool->round_size = 0;
		while (!pool->stail) {
			if (!rlist_empty(&pool->idle)) {
				fiber_call(rlist_shift_entry(&pool->idle, struct fiber, state));
			} else if (pool->used < pool->size) {
				struct fiber *fiber = fiber_new(cord_name(cord()), sbus_fiber_pool_f);
				++pool->used;
				fiber_start(fiber, pool);
			} else {
				pool->stail = 1;
			}
			if (pool->round_size % pool->pool_batch == 0)
				break;
		}
		if (!pool->stail)
			fiber_reschedule();
		else
			fiber_yield();
	}
	sbus_detach_lock(pool->peak);
	//TODO: free all resources
	free(pool);
	return 0;
}

void
sbus_wake_cb(void *arg)
{
	struct sbus_wake_ctx *wake_ctx = (struct sbus_wake_ctx *)arg;
	ev_async_send(wake_ctx->loop, &wake_ctx->async);
}

static void
sbus_wake_func(ev_loop *loop, struct ev_async *watcher, int events)
{
	(void) loop;
	struct fiber *fiber = (struct fiber *)watcher->data;
	(void) events;
	fiber_call(fiber);
}

void
sbus_wake_init(struct sbus_wake_ctx *wake_ctx, struct fiber *fiber)
{
	wake_ctx->loop = cord()->loop;
	ev_async_init(&wake_ctx->async, sbus_wake_func);
	wake_ctx->async.data = fiber;
	ev_async_start(cord()->loop, &wake_ctx->async);
}

struct sbus_pool *
sbus_attach_pool(struct sbus *sbus, char *name, unsigned pool_size,
		  uint32_t pool_batch)
{
	struct sbus_pool *pool = (struct sbus_pool *)calloc(1, sizeof(struct sbus_pool));
	pool->size = pool_size;
	pool->used = 0;
	pool->stail = 0;
	pool->pool_batch = pool_batch;
	rlist_create(&pool->idle);
	pool->sched = fiber_new(cord_name(cord()), fiber_sched_f);
	fiber_set_joinable(pool->sched, true);
	sbus_wake_init(&pool->wake, pool->sched);
	pool->peak = sbus_attach_lock(sbus, name,
				      sbus_wake_cb, &pool->wake);
	fiber_start(pool->sched, pool);
	return pool;
}

int
sbus_detach_pool(struct sbus_pool *pool)
{
	pool->done = true;
	fiber_wakeup(pool->sched);
	fiber_join(pool->sched);
	sbus_detach_lock(pool->peak);
//	free(pool->sched);
	free(pool);
	return 0;
}

