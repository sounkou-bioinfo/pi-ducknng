#pragma once
#include <stdint.h>

#ifndef _WIN32
#include <pthread.h>
typedef pthread_t ducknng_thread;
typedef pthread_mutex_t ducknng_mutex;
typedef pthread_cond_t ducknng_cond;
#else
typedef void *ducknng_thread;
typedef void *ducknng_mutex;
typedef void *ducknng_cond;
#endif

int ducknng_thread_create(ducknng_thread *thread, void *(*fn)(void *), void *arg);
void ducknng_thread_join(ducknng_thread thread);
int ducknng_mutex_init(ducknng_mutex *mu);
void ducknng_mutex_lock(ducknng_mutex *mu);
void ducknng_mutex_unlock(ducknng_mutex *mu);
void ducknng_mutex_destroy(ducknng_mutex *mu);
int ducknng_cond_init(ducknng_cond *cv);
void ducknng_cond_wait(ducknng_cond *cv, ducknng_mutex *mu);
int ducknng_cond_timedwait_ms(ducknng_cond *cv, ducknng_mutex *mu, uint64_t timeout_ms);
void ducknng_cond_signal(ducknng_cond *cv);
void ducknng_cond_broadcast(ducknng_cond *cv);
void ducknng_cond_destroy(ducknng_cond *cv);
