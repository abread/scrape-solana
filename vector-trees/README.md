# Vector backed B and AVL trees written in Rust

[![MIT licensed][mit-badge]][mit-url]

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE.md

The goal was to provide vector backed tree/map implementation before `allocator_api` is stabilized and the standard library supports custom allocators in data structures.

## IMPORTANT

These trees seem stable, but have not been properly tested yet, only fuzzed with RNG. If any of the tests fail, PLEASE submit an issue report with the (last) seed number with which the test failed.

## BVecTreeMap

Follows the same function namings as regular `BTreeMap`, however, so far does not include quite a few of them.

## AVLTree

Very much unstabilized, may even get removed, as it is around 3 times slower than `BTreeMap`. Activate using "avltree" feature flag.
