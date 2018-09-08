**Setup**

1) Install rust https://www.rust-lang.org/en-US/install.html
2) make

**Example**

Validate a 1GB block with 5M transactions and 30% of inputs within the block on 1, 2, and 4 threads
```
./parallel_ttor 1 5000000 200 0 0.3
./parallel_ttor 2 5000000 200 0 0.3
./parallel_ttor 4 5000000 200 0 0.3
````
