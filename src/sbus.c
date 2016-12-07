#include "sbus.h"
#include "small/rlist.h"
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

#include <stdio.h>

#define alloc_align(size, align) \
({	void *ptr; \
	if (posix_memalign(&ptr, align, size)) ptr = NULL; \
	ptr; \
})

#define unlikely(x) __builtin_expect(!!(x), 0)

static void
sbus_changed(struct sbus *sbus)
{
	struct sbus_watcher *watcher;
	while (!(rlist_empty(&sbus->watchers))) {
		watcher = rlist_shift_entry(&sbus->watchers, struct sbus_watcher, item);
		watcher->notify(watcher->notify_arg);
		free(watcher);
	}
}

static int
sbus_watch(struct sbus *sbus, void (*notify)(void *), void *notify_arg)
{
	struct sbus_watcher *watcher;
	watcher = (struct sbus_watcher *)malloc(sizeof(*watcher));
	watcher->notify = notify;
	watcher->notify_arg = notify_arg;
	rlist_add_tail(&sbus->watchers, &watcher->item);
	return 0;
}

struct sbus *
sbus_create()
{
	struct sbus *sbus = (struct sbus *)malloc(sizeof(struct sbus));
	rlist_create(&sbus->peaks);
	rlist_create(&sbus->watchers);
	(void) pthread_mutex_init(&sbus->mutex, NULL);
	return sbus;
}

int
sbus_free(struct sbus *sbus, void (*notify)(void *), void *notify_arg)
{
	pthread_mutex_lock(&sbus->mutex);
	if (!rlist_empty(&sbus->peaks)) {
		if (notify)
			sbus_watch(sbus, notify, notify_arg);
		pthread_mutex_unlock(&sbus->mutex);
		return 1;
	}
	pthread_mutex_unlock(&sbus->mutex);
	pthread_mutex_destroy(&sbus->mutex);
	free(sbus);
	return 0;
}

struct sbus_peak *
sbus_attach(struct sbus *sbus, char *name,
	    void (*ready)(void *), void *ready_arg,
	    void (*notify)(void *), void *notify_arg)
{
	pthread_mutex_lock(&sbus->mutex);
	struct sbus_peak *peak;
	rlist_foreach_entry(peak, &sbus->peaks, item) {
		if (!strcmp(name, peak->name)) {
			if (notify)
				sbus_watch(sbus, notify, notify_arg);
			pthread_mutex_unlock(&sbus->mutex);
			return NULL;
		}
	}

	peak = (struct sbus_peak *)malloc(sizeof(struct sbus_peak));
	peak->sbus = sbus;
	peak->name = strdup(name);
	peak->ready = ready;
	peak->ready_arg = ready_arg;

	peak->route = NULL;
	peak->stail = 1;
	rlist_add(&sbus->peaks, &peak->item);
	sbus_changed(sbus);
	pthread_mutex_unlock(&sbus->mutex);
	return peak;
}

int
sbus_detach(struct sbus_peak *peak,
	    void (*notify)(void *), void *notify_arg)
{
	return 0;
	pthread_mutex_lock(&peak->sbus->mutex);
	if (peak->route == NULL) {
		rlist_del(&peak->item);
		free(peak->name);
		free(peak);
		sbus_changed(peak->sbus);
		pthread_mutex_unlock(&peak->sbus->mutex);
		return 0;
	}
	if (notify)
		sbus_watch(peak->sbus, notify, notify_arg);
	pthread_mutex_unlock(&peak->sbus->mutex);
	return 1;
}

struct sbus_route *
sbus_route(struct sbus *sbus, char *name, unsigned priority,
	   void (*notify)(void *), void *notify_arg)
{
	struct sbus_peak *peak = NULL;
	struct sbus_route *route = NULL;

	pthread_mutex_lock(&sbus->mutex);
	rlist_foreach_entry(peak, &sbus->peaks, item) {
		if (!strcmp(name, peak->name)) {
			route = (struct sbus_route *)malloc(sizeof(*route));
			route->peak = peak;
			route->rpos = 0;
			route->wpos = 0;
			route->exiting = 0;
			route->priority = priority;

			route->rchunk = route->wchunk = (struct sbus_chunk *)alloc_align(sizeof(struct sbus_chunk), 64);
			route->wchunk->next = route->rchunk;

			if (!peak->route) {
				route->next = route;
				peak->route = route;
			} else {
				route->next = peak->route->next;
				peak->route->next = route;
			}
			sbus_changed(sbus);
			pthread_mutex_unlock(&sbus->mutex);
			return route;
		}
	}
	if (notify)
		sbus_watch(sbus, notify, notify_arg);
	pthread_mutex_unlock(&sbus->mutex);
	return NULL;
}

int
sbus_unroute(struct sbus_route *route,
	   void (*notify)(void *), void *notify_arg)
{
	//TODO: check route removal and route walk in get
	if (route->exiting)
		return 1;

	if (route->wpos != route->rpos) {
	/* BUG: raise with get */
		if (notify) {
			route->exit = notify;
			route->exit_arg = notify_arg;
		}
		route->exiting = 1;
		return 1;
	}

	pthread_mutex_lock(&route->peak->sbus->mutex);
	struct sbus_chunk *chunk = route->rchunk;
	do {
		struct sbus_chunk *pchunk = chunk;
		chunk = chunk->next;
		free(pchunk);
	} while (chunk != route->rchunk);
	/* use two way list */
	struct sbus_route *proute = route;
	while (proute->next != route)
		proute = proute->next;
	if (proute == route)
		route->peak->route = NULL;
	else
		route->peak->route = proute->next = route->next;
	free(route);
	sbus_changed(route->peak->sbus);
	pthread_mutex_unlock(&route->peak->sbus->mutex);
	return 0;
}

void *
sbus_get(struct sbus_peak *peak)
{
	struct sbus_route *route = peak->route;
	if (unlikely(!route)) {
		return NULL;
	}
	if (route->rpos == route->wpos) {
		if (!__sync_bool_compare_and_swap(&peak->stail, 0, 1))
			return NULL;
		struct sbus_route *old_route = route;
		while (route->next != old_route) {
			route = route->next;
			if (!(route->rpos == route->wpos))
				break;
		}
		if (route->rpos == route->wpos) {
			return NULL;
		}
	}
	/* TODO: check exiting */
	peak->stail = 0;
	struct sbus_chunk *chunk = route->rchunk;
	void *msg = chunk->messages[route->rpos & SBUS_CHUNK_MASK];
	++route->rpos;
	if (unlikely(!(route->rpos & SBUS_CHUNK_MASK))) {
		route->rchunk = chunk->next;
	}
	if (unlikely(!(route->rpos % route->priority)))
		route = route->next;
	peak->route = route;
	return msg;
}

unsigned int
sbus_get_many(struct sbus_peak *peak, void **data, unsigned int n)
{
	if (!__sync_bool_compare_and_swap(&peak->stail, 0, 1))
			return 0;
	struct sbus_route *route = peak->route;
	if (unlikely(!route)) {
		return 0;
	}
	unsigned int cnt = 0;

	unsigned int found = 0;
	do {

		if (route == peak->route) {
			found = 0;
		}
		if (route->wpos > route->rpos) {

			peak->stail = 0;
			found = 1;
			unsigned int delta = route->wpos - route->rpos;
			uint64_t rpos = route->rpos & SBUS_CHUNK_MASK;
			delta = delta > (SBUS_CHUNK_SIZE - rpos) ? (SBUS_CHUNK_SIZE - rpos) : delta;
			delta = delta > n - cnt ? n - cnt : delta;

			memcpy(data + cnt, route->rchunk->messages + rpos, sizeof(void *) * delta);
			route->rpos += delta;
			if (!(route->rpos & SBUS_CHUNK_MASK)) {
				route->rchunk = route->rchunk->next;
			}
			cnt += delta;
		}
		route = route->next;
	}
	while ((cnt < n) && (found || route != peak->route));
	return cnt;
}

int sbus_put_start(struct sbus_route *route, void *msg)
{
	if (route->exiting)
		return 1;
	uint64_t wpos = (route->wpos & SBUS_CHUNK_MASK);
	struct sbus_chunk *chunk = route->wchunk;
	if (unlikely(wpos == SBUS_CHUNK_MASK)) {
		if (chunk->next == route->rchunk) {
			struct sbus_chunk *new_chunk = (struct sbus_chunk *)alloc_align(sizeof(struct sbus_chunk), 8);
			new_chunk->next = chunk->next;
			chunk->next = new_chunk;
		}
		route->wchunk = chunk->next;
	}
	chunk->messages[wpos] = msg;
	__asm__ volatile ("" : : : "memory");
	route->wpos++;
	return 0;
}

int sbus_put_done(struct sbus_route *route)
{
	struct sbus_peak *peak = route->peak;

	/* this can be expensive */
	if (__sync_bool_compare_and_swap(&peak->stail, 1, 0) && peak->ready)
		peak->ready(peak->ready_arg);

	return 0;
}

int
sbus_put(struct sbus_route *route, void *msg)
{
	int res;
	if ((res = sbus_put_start(route, msg)))
		return res;
	return sbus_put_done(route);
}


