##  
## Memcached Test
##

assert("Memcached#set,get with string") do
  m = Memcached.new "127.0.0.1:11211"
  m.set "test", "1"
  real = m.get "test"
  m.close
  assert_equal("1", real)
end

assert("Memcached#set,get with symbol") do
  m = Memcached.new "127.0.0.1:11211"
  m.set :hoge, 10
  real = m.get :hoge
  m.close
  assert_equal("10", real)
end

assert("Memcached#set,get with symbol and string") do
  m = Memcached.new "127.0.0.1:11211"
  m.set "foo", 100
  real = m.get :foo
  m.close
  assert_equal("100", real)
end
