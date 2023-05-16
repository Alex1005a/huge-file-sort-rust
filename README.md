This is an attempt to implement an external merge sort from an article https://habr.com/ru/articles/714524/ in the Rust language.

At the moment, the speed of sorting a 160 mb file takes 6 seconds on the rust version, while on the c# version it takes 4 seconds

Sort keys are not involved at this time and not all sorting parts are parallelized.