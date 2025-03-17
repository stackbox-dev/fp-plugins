# Stackbox Nodejs Standard Utility

### Install

- All Stackbox Node.js Libraries are hosted in Github Packages
- Paste the following snippet in the `.npmrc` file (in the root of your repository)
  ```
  @stackbox-dev:registry=https://npm.pkg.github.com
  //npm.pkg.github.com/:_authToken="\u0067hp_RKborEB8lGZp00uMv7i0d8Oo202jHK3syROX"
  ```

### Modules

#### Graph Algorithms

A collection of algorithms for working with graphs, including cycle detection, topological sorting, distance matrix calculation, and graph closure operations. These utilities are useful for solving various graph-related problems in computer science and network analysis.

#### Lang (lang.ts)

A set of utility functions for common operations on collections and data structures. This module provides a range of helper methods to simplify tasks such as sorting, grouping, finding extrema, and manipulating arrays and objects in JavaScript/TypeScript.

#### Geo (geo.ts)

Geographic and geometric utility functions for working with spatial data. This module offers tools for performing calculations and operations on geographic coordinates and shapes, which can be valuable for mapping applications and location-based services.

#### BinPack (binpack.ts)

Implementation of bin packing algorithms, specifically the First-Fit Decreasing method. This module is useful for optimizing resource allocation problems, such as efficiently packing items into containers or scheduling tasks with limited resources.

#### URL (url.ts)

Utilities for working with URLs, providing functions to parse, manipulate, and validate web addresses. This module can be helpful in web development tasks, API integrations, and handling URL-related operations.

#### BigInt (bigint.ts)

A module for working with arbitrary-precision integers in JavaScript/TypeScript. It provides operations and utilities for handling very large numbers that exceed the limits of the standard Number type, useful in cryptography, scientific computing, and financial calculations.

#### Random (rand.ts)

A collection of functions for generating random numbers, selecting random elements from arrays, and creating other randomized data. This module can be valuable for simulations, testing, and any application requiring controlled randomness.
