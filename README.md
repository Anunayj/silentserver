# Silent Payment Server

## Overview
Silent Payment Server is a Rust-based implementation that enables indexing of transactions on the Bitcoin network for silent payments. This project leverages:

- [`libbitcoinkernel`](https://github.com/TheCharlatan/rust-bitcoinkernel) for Bitcoin transaction and block validation.
- The `silentpayment` library to scan and index transactions for silent payments.

The network protocol implementation for transaction propagation and peer communication is still a **TODO**.

## Dependencies

Ensure you have the following installed:

- Rust – [Install Rust](https://www.rust-lang.org/tools/install)
- `cmake` (if not already installed)
- a working C and C++ compiler

## Building

Clone the repository and build the project with:

```sh
cargo build --release
```

## Running the Server

Once built, run the server using:

```sh
target/release/silent-payment-server
```

By default, it will start indexing transactions based on silent payment rules. Network protocol functionality is yet to be implemented.

## TODO

- Implement a Transport Protocol for serving processed block data.

## License

This project is licensed under the MIT License.

