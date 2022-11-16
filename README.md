# GStream

## Benchmark

```
goos: linux
goarch: amd64
pkg: github.com/KumKeeHyun/gstream
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
BenchmarkGenericSize1000-4            	   46689	     24067 ns/op	   33016 B/op	       3 allocs/op
BenchmarkGStreamSize1000-4            	    3979	    303984 ns/op	   31595 B/op	      63 allocs/op
BenchmarkGenericSize100000-4          	     381	   2726237 ns/op	 4276250 B/op	       4 allocs/op
BenchmarkGStreamSize100000-4          	      42	  26607581 ns/op	 3878168 B/op	      68 allocs/op
BenchmarkGeneric10TimesSize1000-4     	      21	  51791600 ns/op	61577996 B/op	      49 allocs/op
BenchmarkGStream10TimesSize1000-4     	    2452	    474098 ns/op	   35988 B/op	     207 allocs/op
BenchmarkGStream10TimesSize100000-4   	      28	  40920763 ns/op	 3882629 B/op	     212 allocs/op
PASS
ok  	github.com/KumKeeHyun/gstream	15.228s
```