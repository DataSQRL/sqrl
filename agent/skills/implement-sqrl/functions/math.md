# Math Functions

| Function | Description |
|----------|-------------|
| `cbrt(double)` | Calculates the cube root of a number. For example, `cbrt(27.0)` returns `3.0`, which is the cube root of 27.0. |
| `copy_sign(double, double)` | Returns the first argument with the sign of the second argument. For example, `copy_sign(2.0, -3.0)` returns `-2.0`. |
| `expm1(double)` | Calculates e^x - 1 with better precision for small values. For example, `expm1(0.0)` returns `0.0`, as `e^0 - 1 = 0`. |
| `hypot(double, double)` | Computes sqrt(x² + y²) without intermediate overflow or underflow. For example, `hypot(3.0, 4.0)` returns `5.0`, which is the hypotenuse of a 3-4-5 triangle. |
| `log1p(double)` | Computes the natural logarithm of 1 + x (log(1 + x)) accurately for small x. For example, `log1p(0.0)` returns `0.0` as `log(1 + 0) = 0`. |
| `next_after(double, double)` | Returns the next floating-point number towards the direction of the second argument. For example, `next_after(1.0, 2.0)` returns the next representable number after 1.0. |
| `scalb(double, bigint)` | Multiplies a floating-point number by 2 raised to the power of an integer. For example, `scalb(1.0, 3)` returns `8.0` as `1.0 * 2^3 = 8.0`. |
| `ulp(double)` | Returns the size of the unit in the last place (ULP) of the argument. For example, `ulp(1.0)` returns the ULP of 1.0. |
| `binomial_distribution(bigint, double, bigint)` | Calculates the probability of obtaining a number of successes in a fixed number of trials for a binomial distribution. For example, `binomial_distribution(10, 0.5, 5)` returns the probability of 5 successes out of 10 trials with a 50% success rate. |
| `exponential_distribution(double, double)` | Evaluates the probability density or cumulative distribution of an exponential distribution. For example, `exponential_distribution(1.0, 2.0)` returns the exponential distribution's probability for a given rate and time. |
| `normal_distribution(double, double, double)` | Evaluates the cumulative distribution function for a normal (Gaussian) distribution. For example, `normal_distribution(0.0, 1.0, 1.0)` returns the probability for a standard normal distribution with mean 0 and standard deviation 1. |
| `poisson_distribution(double, bigint)` | Evaluates the probability mass function of a Poisson-distributed random variable. For example, `poisson_distribution(1.0, 5)` returns the probability of observing 5 events when the average event rate is 1.0. |
