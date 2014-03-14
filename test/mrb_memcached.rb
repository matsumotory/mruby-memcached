##  
## Memcached Test
##

assert("Memcached#set,get with string") do
  m = Memcached.new "127.0.0.1:11211"
  m.flush
  m.set "test", "1"
  real = m.get "test"
  m.close
  assert_equal("1", real)
end

assert("Memcached#[],[]= with string") do
  m = Memcached.new "127.0.0.1:11211"
  m.flush
  m["test"] = 1
  real = m["test"]
  m.close
  assert_equal("1", real)
end

assert("Memcached#set,get with symbol") do
  m = Memcached.new "127.0.0.1:11211"
  m.flush
  m.set :hoge, 10
  real = m.get :hoge
  m.close
  assert_equal("10", real)
end

assert("Memcached#set,get with symbol and string") do
  m = Memcached.new "127.0.0.1:11211"
  m.flush
  m.set "foo", 100
  real = m.get :foo
  m.close
  assert_equal("100", real)
end

assert("Memcached#delete") do
  m = Memcached.new "127.0.0.1:11211"
  m.flush
  m.set "foo", 100
  m.delete :foo
  real = m.get :foo
  m.close
  assert_equal(nil, real)
end

assert("Memcached#add") do
  m = Memcached.new "127.0.0.1:11211"
  m.flush
  m.set "foo", 100
  real1 = m.add :foo, 101
  real2 = m.add :foo1, 10
  real3 = m.get :foo1
  real4 = m.get :foo
  m.close
  assert_equal(Memcached::MEMCACHED_NOTSTORED, real1)
  assert_equal(Memcached::MEMCACHED_SUCCESS, real2)
  assert_equal("10", real3)
  assert_equal("100", real4)
end

assert("Memcached#flush") do
  m = Memcached.new "127.0.0.1:11211"
  m.set "foo", 100
  real1 = m.get :foo
  m.flush
  real2 = m.get :foo
  m.close
  assert_equal("100", real1)
  assert_equal(nil, real2)
end
