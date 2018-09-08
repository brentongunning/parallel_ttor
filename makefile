all:
	cargo build --release
	cp target/release/parallel_ttor .
	strip parallel_ttor

clean:
	cargo clean
	rm parallel_ttor
