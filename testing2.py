# import xlsx_generator as tablegen
import polars as pl

# print(tablegen.pull_data(
#     search_type="Fakulta",
#     search_target="PRF",
#     ticket_over="56ac36a08e6d8d1fd3aa7579b23064c3402f963e3b7d3fd1fb2a03197a555050"
# ))

my_df = pl.DataFrame({
    "Hi":["Hello", "Ahoj", "Hallo"],
    "Bye":range(3),
    "Maybe":[True, False, True]
})