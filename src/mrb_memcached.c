/*
** mrb_memcached.c - Memcached class
**
** Copyright (c) MATSUMOTO Ryosuke 2014
**
** See Copyright Notice in LICENSE
*/

#include "mruby.h"
#include "mruby/data.h"
#include "mruby/string.h"
#include "mrb_memcached.h"
#include "libmemcached/memcached.h"

#define DONE mrb_gc_arena_restore(mrb, 0);

typedef struct {
  memcached_st *mst;
} mrb_memcached_data;

static void mrb_memcached_data_free(mrb_state *mrb, void *p)
{   
  mrb_memcached_data *data = (mrb_memcached_data *)p;
  memcached_free(data->mst);
}

static const struct mrb_data_type mrb_memcached_data_type = {
  "mrb_memcached_data", mrb_memcached_data_free,
};

static mrb_value mrb_memcached_init(mrb_state *mrb, mrb_value self)
{
  mrb_memcached_data *data;
  memcached_server_st *msv;
  memcached_st *mst;
  memcached_return mrt;
  const char *servers;

  data = (mrb_memcached_data *)DATA_PTR(self);
  if (data) {
    mrb_free(mrb, data);
  }
  DATA_TYPE(self) = &mrb_memcached_data_type;
  DATA_PTR(self) = NULL;

  mrb_get_args(mrb, "z", &servers);

  mst = memcached_create(NULL);  
  msv = memcached_servers_parse(servers);
  mrt = memcached_server_push(mst, msv);
  if (mrt != MEMCACHED_SUCCESS) {
    mrb_raisef(mrb, E_RUNTIME_ERROR, "libmemcached error: %S"
      , mrb_str_new_cstr(mrb, memcached_strerror(mst, mrt)));
  }
  memcached_server_list_free(msv);

  data = (mrb_memcached_data *)mrb_malloc(mrb, sizeof(mrb_memcached_data));
  data->mst = mst;
  DATA_PTR(self) = data;

  return self;
}

static mrb_value mrb_memcached_server_add(mrb_state *mrb, mrb_value self)
{
  mrb_memcached_data *data = DATA_PTR(self);
  char *host;
  mrb_int port;
  memcached_return mrt;

  mrb_get_args(mrb, "zi", &host, &port);
  mrt = memcached_server_add(data->mst, host, port);
  if (mrt != MEMCACHED_SUCCESS) {
    // can't add server to memcached server list
    mrb_raisef(mrb, E_RUNTIME_ERROR, "libmemcached error: %S"
      , mrb_str_new_cstr(mrb, memcached_strerror(data->mst, mrt)));
  }

  return mrb_fixnum_value(mrt);
}

static mrb_value mrb_memcached_close(mrb_state *mrb, mrb_value self)
{
  mrb_memcached_data *data = DATA_PTR(self);
  memcached_free(data->mst);
  return self;
}

static mrb_value mrb_memcached_set(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  mrb_int expr = 0;
  mrb_memcached_data *data = DATA_PTR(self);
  memcached_return mrt;

  mrb_get_args(mrb, "oo|i", &key, &val, &expr);
  switch (mrb_type(key)) {
    case MRB_TT_STRING:
      break;
    case MRB_TT_SYMBOL:
      key = mrb_obj_as_string(mrb, key);
      break;
    default:
      mrb_raise(mrb, E_RUNTIME_ERROR, "memcached key type is string or symbol");
  }
  val = mrb_obj_as_string(mrb, val);

  mrt = memcached_set(data->mst, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(val), RSTRING_LEN(val), (time_t)expr, (uint32_t)0);
  if (mrt != MEMCACHED_SUCCESS && mrt != MEMCACHED_BUFFERED) {
    // set failed
    return mrb_nil_value();
  }
  return mrb_fixnum_value(mrt);
}

static mrb_value mrb_memcached_add(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  mrb_int expr = 0;
  mrb_memcached_data *data = DATA_PTR(self);
  memcached_return mrt;

  mrb_get_args(mrb, "oo|i", &key, &val, &expr);
  switch (mrb_type(key)) {
    case MRB_TT_STRING:
      break;
    case MRB_TT_SYMBOL:
      key = mrb_obj_as_string(mrb, key);
      break;
    default:
      mrb_raise(mrb, E_RUNTIME_ERROR, "memcached key type is string or symbol");
  }
  val = mrb_obj_as_string(mrb, val);

  mrt = memcached_add(data->mst, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(val), RSTRING_LEN(val), (time_t)expr, (uint32_t)0);
  if (mrt != MEMCACHED_SUCCESS && mrt != MEMCACHED_STORED && mrt != MEMCACHED_NOTSTORED) {
    // add failed
    return mrb_nil_value();
  }
  return mrb_fixnum_value(mrt);
}

static mrb_value mrb_memcached_get(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  char *val;
  size_t len;
  uint32_t flags;
  memcached_return mrt;
  mrb_memcached_data *data = DATA_PTR(self);

  mrb_get_args(mrb, "o", &key);
  switch (mrb_type(key)) {
    case MRB_TT_STRING:
      break;
    case MRB_TT_SYMBOL:
      key = mrb_obj_as_string(mrb, key);
      break;
    default:
      mrb_raise(mrb, E_RUNTIME_ERROR, "memcached key type is string or symbol");
  }
  val = memcached_get(data->mst, RSTRING_PTR(key), RSTRING_LEN(key), &len, &flags, &(mrt));
  if (mrt != MEMCACHED_SUCCESS && mrt != MEMCACHED_BUFFERED) {
    free(val);
    // value not found
    return mrb_nil_value();
  }
  return mrb_str_new(mrb, val, len);
}

static mrb_value mrb_memcached_delete(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  memcached_return mrt;
  mrb_int expr = 0;
  mrb_memcached_data *data = DATA_PTR(self);

  mrb_get_args(mrb, "o|i", &key, &expr);
  switch (mrb_type(key)) {
    case MRB_TT_STRING:
      break;
    case MRB_TT_SYMBOL:
      key = mrb_obj_as_string(mrb, key);
      break;
    default:
      mrb_raise(mrb, E_RUNTIME_ERROR, "memcached key type is string or symbol");
  }
  mrt = memcached_delete(data->mst, RSTRING_PTR(key), RSTRING_LEN(key), (time_t)expr);
  if (mrt != MEMCACHED_SUCCESS && mrt != MEMCACHED_BUFFERED) {
    return mrb_nil_value();
  }
  return mrb_fixnum_value(mrt);
}

static mrb_value mrb_memcached_behavior_set(mrb_state *mrb, mrb_value self)
{
  mrb_memcached_data *data = DATA_PTR(self);
  uint64_t fdata = 1;
  memcached_behavior_t flag;
  memcached_return mrt;

  mrb_get_args(mrb, "i|i", &flag, &fdata);
  mrt = memcached_behavior_set(data->mst, flag, fdata);
  if (mrt != MEMCACHED_SUCCESS) {
    return mrb_nil_value();
  }
  return mrb_fixnum_value(mrt);
}

static mrb_value mrb_memcached_flush(mrb_state *mrb, mrb_value self)
{
  memcached_return mrt;
  mrb_memcached_data *data = DATA_PTR(self);
  mrb_int expr = 0;

  mrb_get_args(mrb, "|i", &expr);
  mrt = memcached_flush(data->mst, expr);
  if (mrt != MEMCACHED_SUCCESS) {
    return mrb_nil_value();
  }
  return mrb_fixnum_value(mrt);
}

static mrb_value mrb_memcached_flush_buffers(mrb_state *mrb, mrb_value self)
{
  memcached_return mrt;
  mrb_memcached_data *data = DATA_PTR(self);

  mrt = memcached_flush_buffers(data->mst);
  if (mrt != MEMCACHED_SUCCESS) {
    return mrb_nil_value();
  }
  return mrb_fixnum_value(mrt);
}

#define MRB_MEMCACHED_DEFINE_CONST_FIXNUM(val)  mrb_define_const(mrb, memcached, #val, mrb_fixnum_value(val))

void mrb_mruby_memcached_gem_init(mrb_state *mrb)
{
  struct RClass *memcached;
  memcached = mrb_define_class(mrb, "Memcached", mrb->object_class);

  mrb_define_method(mrb, memcached, "initialize", mrb_memcached_init, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, memcached, "server_add", mrb_memcached_server_add, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, memcached, "close", mrb_memcached_close, MRB_ARGS_NONE());
  mrb_define_method(mrb, memcached, "set", mrb_memcached_set, MRB_ARGS_ANY());
  mrb_define_method(mrb, memcached, "[]=", mrb_memcached_set, MRB_ARGS_ANY());
  mrb_define_method(mrb, memcached, "add", mrb_memcached_add, MRB_ARGS_ANY());
  mrb_define_method(mrb, memcached, "get", mrb_memcached_get, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, memcached, "[]", mrb_memcached_get, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, memcached, "delete", mrb_memcached_delete, MRB_ARGS_ANY());
  mrb_define_method(mrb, memcached, "behavior_set", mrb_memcached_behavior_set, MRB_ARGS_ANY());
  mrb_define_method(mrb, memcached, "flush", mrb_memcached_flush, MRB_ARGS_OPT(1));
  mrb_define_method(mrb, memcached, "flush_buffers", mrb_memcached_flush_buffers, MRB_ARGS_NONE());

  // behavior type, enum
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_NO_BLOCK);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_TCP_NODELAY);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_HASH);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_KETAMA);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_CACHE_LOOKUPS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_SUPPORT_CAS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_POLL_TIMEOUT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_DISTRIBUTION);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_BUFFER_REQUESTS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_USER_DATA);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_SORT_HOSTS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_VERIFY_KEY);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_RETRY_TIMEOUT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_KETAMA_HASH);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_BINARY_PROTOCOL);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_SND_TIMEOUT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_RCV_TIMEOUT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_IO_MSG_WATERMARK);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_IO_BYTES_WATERMARK);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_IO_KEY_PREFETCH);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_HASH_WITH_PREFIX_KEY);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_NOREPLY);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_USE_UDP);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_NUMBER_OF_REPLICAS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_RANDOMIZE_REPLICA_READ);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_CORK);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_TCP_KEEPALIVE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_TCP_KEEPIDLE);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_LOAD_FROM_FILE);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_REMOVE_FAILED_SERVERS);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_DEAD_TIMEOUT);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BEHAVIOR_MAX);
  
  // return type, enum 
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_SUCCESS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_HOST_LOOKUP_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_CONNECTION_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_CONNECTION_BIND_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_WRITE_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_READ_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_UNKNOWN_READ_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_PROTOCOL_ERROR);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_CLIENT_ERROR);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_SERVER_ERROR);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_ERROR);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_DATA_EXISTS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_DATA_DOES_NOT_EXIST);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_NOTSTORED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_STORED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_NOTFOUND);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_MEMORY_ALLOCATION_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_PARTIAL_READ);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_SOME_ERRORS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_NO_SERVERS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_END);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_DELETED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_VALUE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_STAT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_ITEM);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_ERRNO);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_FAIL_UNIX_SOCKET);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_NOT_SUPPORTED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_NO_KEY_PROVIDED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_FETCH_NOTFINISHED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_TIMEOUT);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BUFFERED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_BAD_KEY_PROVIDED);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_INVALID_HOST_PROTOCOL);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_SERVER_MARKED_DEAD);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_UNKNOWN_STAT_KEY);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_E2BIG);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_INVALID_ARGUMENTS);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_KEY_TOO_BIG);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_AUTH_PROBLEM);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_AUTH_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_AUTH_CONTINUE);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_PARSE_ERROR);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_PARSE_USER_ERROR);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_DEPRECATED);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_IN_PROGRESS);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_SERVER_TEMPORARILY_DISABLED);
  //MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_SERVER_MEMORY_ALLOCATION_FAILURE);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_MAXIMUM_RETURN);
  MRB_MEMCACHED_DEFINE_CONST_FIXNUM(MEMCACHED_CONNECTION_SOCKET_CREATE_FAILURE);

  DONE;
}

void mrb_mruby_memcached_gem_final(mrb_state *mrb)
{
}

