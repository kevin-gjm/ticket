/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief ADT for managing Raft log entries (aka entries)
 * @author Willem Thiart himself@willemthiart.com
 * @version 0.1
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"
#include "raft_log.h"

#define INITIAL_CAPACITY 10
#define in(x) ((log_private_t*)x)

typedef struct
{

	/* base_log_idx + count = back */
    /* the amount of elements without compact elements */
    unsigned long count;

	/* the amount of elements */
	unsigned long back;

	/* we compact the log, and thus need to increment the base idx */
	unsigned long base_log_idx;
	
    /* position of the queue */
    unsigned long front;

    /* callbacks */
    raft_cbs_t *cb;
    void* raft;
} log_private_t;



log_t* log_new()
{
    log_private_t* me = (log_private_t*)calloc(1, sizeof(log_private_t));
    me->count = 0;
    me->back = in(me)->front = 0;
	me->base_log_idx=0;
    return (log_t*)me;
}

int set_log_count(log_t* me_,unsigned long total_amount,unsigned long hold_count)
{
	 log_private_t* me = (log_private_t*)me_;
	 me->back=total_amount;
	 me->count=me->back;
	 return 0;
}


void log_set_callbacks(log_t* me_, raft_cbs_t* funcs, void* raft)
{
    log_private_t* me = (log_private_t*)me_;

    me->raft = raft;
    me->cb = funcs;
}

int log_append_entry(log_t* me_, raft_entry_t* c)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == c->id)
        return -1;


    if (me->cb && me->cb->log_offer)
        me->cb->log_offer(me->raft, raft_get_udata(me->raft), c, me->back);
    me->count++;
    me->back++;
    return 0;
}

int log_get_from_idx(log_t* me_, unsigned long idx, raft_entry_t* ety)
{
    log_private_t* me = (log_private_t*)me_;
    int i;

    assert(0 <= idx - 1);

    if (me->base_log_idx + me->count < idx || idx < me->base_log_idx)
        return -1;

    /* idx starts at 1 */
    idx -= 1;

    //i = (me->front + idx - me->base_log_idx) % me->size;
     if (me->cb && me->cb->log_get)
     {
     	me->cb->log_get(me->raft, raft_get_udata(me->raft), ety, idx);
		return 0;
     }
	 return -1; 
}

unsigned long log_count(log_t* me_)
{
    return ((log_private_t*)me_)->count;
}

int log_delete(log_t* me_, unsigned long idx)
{
    log_private_t* me = (log_private_t*)me_;
    int end;
	
	assert(0 <= idx - 1);

	if (me->base_log_idx + me->count < idx || idx < me->base_log_idx)
		return -1;

    /* idx starts at 1 */
    idx -= 1;


    for (end = log_count(me_); idx < end; idx++)
    {   
    	me->back--;
        me->count--;
  
        if (me->cb && me->cb->log_pop)
            me->cb->log_pop(me->raft, raft_get_udata(me->raft),me->back);
    }
	return 0;
}

int log_poll(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == log_count(me_))
        return -1;

    if (me->cb && me->cb->log_poll)
        me->cb->log_poll(me->raft, raft_get_udata(me->raft), me->front);
    me->front++;
    me->count--;
    me->base_log_idx++;
    return 0;
}


void log_empty(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    me->front = 0;
    me->back = 0;
    me->count = 0;
}

void log_free(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    free(me);
}

unsigned long log_get_current_idx(log_t* me_)
{
    log_private_t* me = (log_private_t*)me_;
    return log_count(me_) + me->base_log_idx;
}
