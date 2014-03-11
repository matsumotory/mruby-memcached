##  
## Memcached Test
##

assert("Memcached#set,get") do
  m = Memcached.new "127.0.0.1", 11211
  m.set "test", "1"
  real = m.get "test"
  m.close
  assert_equal("1", real)
end
