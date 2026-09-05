Python code from ../docs/demos/examples/duckdb/accuracy_analysis_from_labels_column.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import splink_datasets

df = splink_datasets.fake_1000
df.head(2)

from splink import SettingsCreator, Linker, block_on, DuckDBAPI

import splink.comparison_library as cl

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
        block_on("dob"),
        block_on("email"),
    ],
    comparisons=[
        cl.ForenameSurnameComparison("first_name", "surname"),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=True,
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    retain_intermediate_calculation_columns=True,
)

db_api = DuckDBAPI()
linker = Linker(df, settings, db_api=db_api)
deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob, l.dob) <= 1",
    "l.surname = r.surname and levenshtein(r.dob, l.dob) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    "l.email = r.email",
]

linker.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.7
)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6, seed=5)

session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("dob"), estimate_without_term_frequencies=True
)
session_email = linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("email"), estimate_without_term_frequencies=True
)
session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("first_name", "surname"), estimate_without_term_frequencies=True
)

linker.evaluation.accuracy_analysis_from_labels_column(
    "cluster", output_type="table"
).as_pandas_dataframe(limit=5)

linker.evaluation.accuracy_analysis_from_labels_column("cluster", output_type="roc")

linker.evaluation.accuracy_analysis_from_labels_column(
    "cluster",
    output_type="threshold_selection",
    threshold_match_probability=0.5,
    add_metrics=["f1"],
)

# Plot some false positives
linker.evaluation.prediction_errors_from_labels_column(
    "cluster", include_false_negatives=True, include_false_positives=True
).as_pandas_dataframe(limit=5)

records = linker.evaluation.prediction_errors_from_labels_column(
    "cluster", include_false_negatives=True, include_false_positives=True
).as_record_dict(limit=5)

linker.visualisations.waterfall_chart(records)



Python code from ../docs/demos/examples/duckdb/pairwise_labels.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink.datasets import splink_dataset_labels

pairwise_labels = splink_dataset_labels.fake_1000_labels

# Choose labels indicating a match
pairwise_labels = pairwise_labels[pairwise_labels["clerical_match_score"] == 1]
pairwise_labels

from splink import splink_datasets

df = splink_datasets.fake_1000
df.head(2)

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    comparisons=[
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=True,
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    retain_intermediate_calculation_columns=True,
)

linker = Linker(df, settings, db_api=DuckDBAPI(), set_up_basic_logging=False)
deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob, l.dob) <= 1",
    "l.surname = r.surname and levenshtein(r.dob, l.dob) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    "l.email = r.email",
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

# Register the pairwise labels table with the database, and then use it to estimate the m values
labels_df = linker.table_management.register_labels_table(pairwise_labels, overwrite=True)
linker.training.estimate_m_from_pairwise_labels(labels_df)


# If the labels table already existing in the dataset you could run
# linker.training.estimate_m_from_pairwise_labels("labels_tablename_here")

training_blocking_rule = block_on("first_name")
linker.training.estimate_parameters_using_expectation_maximisation(training_blocking_rule)

linker.visualisations.parameter_estimate_comparisons_chart()

linker.visualisations.match_weights_chart()



Python code from ../docs/demos/examples/duckdb/link_only.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import splink_datasets

df = splink_datasets.fake_1000

# Split a simple dataset into two, separate datasets which can be linked together.
df_l = df.sample(frac=0.5)
df_r = df.drop(df_l.index)

df_l.head(2)

import splink.comparison_library as cl

from splink import DuckDBAPI, Linker, SettingsCreator, block_on

settings = SettingsCreator(
    link_type="link_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    comparisons=[
        cl.NameComparison(
            "first_name",
        ),
        cl.NameComparison("surname"),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=True,
            invalid_dates_as_null=True,
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
)

linker = Linker(
    [df_l, df_r],
    settings,
    db_api=DuckDBAPI(),
    input_table_aliases=["df_left", "df_right"],
)

from splink.exploratory import completeness_chart

completeness_chart(
    [df_l, df_r],
    cols=["first_name", "surname", "dob", "city", "email"],
    db_api=DuckDBAPI(),
    table_names_for_chart=["df_left", "df_right"],
)


deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob, l.dob) <= 1",
    "l.surname = r.surname and levenshtein(r.dob, l.dob) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    block_on("email"),
]


linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)

session_dob = linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))
session_email = linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("email")
)
session_first_name = linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("first_name")
)

results = linker.inference.predict(threshold_match_probability=0.9)

results.as_pandas_dataframe(limit=5)



Python code from ../docs/demos/examples/duckdb/transactions.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

df_origin = splink_datasets.transactions_origin
df_destination = splink_datasets.transactions_destination

display(df_origin.head(2))
display(df_destination.head(2))

from splink.exploratory import profile_columns

db_api = DuckDBAPI()
profile_columns(
    [df_origin, df_destination],
    db_api=db_api,
    column_expressions=[
        "memo",
        "transaction_date",
        "amount",
    ],
)

from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

# Design blocking rules that allow for differences in transaction date and amounts
blocking_rule_date_1 = """
    strftime(l.transaction_date, '%Y%m') = strftime(r.transaction_date, '%Y%m')
    and substr(l.memo, 1,3) = substr(r.memo,1,3)
    and l.amount/r.amount > 0.7   and l.amount/r.amount < 1.3
"""

# Offset by half a month to ensure we capture case when the dates are e.g. 31st Jan and 1st Feb
blocking_rule_date_2 = """
    strftime(l.transaction_date+15, '%Y%m') = strftime(r.transaction_date, '%Y%m')
    and substr(l.memo, 1,3) = substr(r.memo,1,3)
    and l.amount/r.amount > 0.7   and l.amount/r.amount < 1.3
"""

blocking_rule_memo = block_on("substr(memo,1,9)")

blocking_rule_amount_1 = """
round(l.amount/2,0)*2 = round(r.amount/2,0)*2 and yearweek(r.transaction_date) = yearweek(l.transaction_date)
"""

blocking_rule_amount_2 = """
round(l.amount/2,0)*2 = round((r.amount+1)/2,0)*2 and yearweek(r.transaction_date) = yearweek(l.transaction_date + 4)
"""

blocking_rule_cheat = block_on("unique_id")


brs = [
    blocking_rule_date_1,
    blocking_rule_date_2,
    blocking_rule_memo,
    blocking_rule_amount_1,
    blocking_rule_amount_2,
    blocking_rule_cheat,
]


db_api = DuckDBAPI()

cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=[df_origin, df_destination],
    blocking_rules=brs,
    db_api=db_api,
    link_type="link_only"
)

# Full settings for linking model
import splink.comparison_level_library as cll
import splink.comparison_library as cl

comparison_amount = {
    "output_column_name": "amount",
    "comparison_levels": [
        cll.NullLevel("amount"),
        cll.ExactMatchLevel("amount"),
        cll.PercentageDifferenceLevel("amount", 0.01),
        cll.PercentageDifferenceLevel("amount", 0.03),
        cll.PercentageDifferenceLevel("amount", 0.1),
        cll.PercentageDifferenceLevel("amount", 0.3),
        cll.ElseLevel(),
    ],
    "comparison_description": "Amount percentage difference",
}

# The date distance is one sided becaause transactions should only arrive after they've left
# As a result, the comparison_template_library date difference functions are not appropriate
within_n_days_template = "transaction_date_r - transaction_date_l <= {n} and transaction_date_r >= transaction_date_l"

comparison_date = {
    "output_column_name": "transaction_date",
    "comparison_levels": [
        cll.NullLevel("transaction_date"),
        {
            "sql_condition": within_n_days_template.format(n=1),
            "label_for_charts": "1 day",
        },
        {
            "sql_condition": within_n_days_template.format(n=4),
            "label_for_charts": "<=4 days",
        },
        {
            "sql_condition": within_n_days_template.format(n=10),
            "label_for_charts": "<=10 days",
        },
        {
            "sql_condition": within_n_days_template.format(n=30),
            "label_for_charts": "<=30 days",
        },
        cll.ElseLevel(),
    ],
    "comparison_description": "Transaction date days apart",
}


settings = SettingsCreator(
    link_type="link_only",
    probability_two_random_records_match=1 / len(df_origin),
    blocking_rules_to_generate_predictions=[
        blocking_rule_date_1,
        blocking_rule_date_2,
        blocking_rule_memo,
        blocking_rule_amount_1,
        blocking_rule_amount_2,
        blocking_rule_cheat,
    ],
    comparisons=[
        comparison_amount,
        cl.LevenshteinAtThresholds("memo", [2, 6, 10]),
        comparison_date,
    ],
    retain_intermediate_calculation_columns=True,
)

linker = Linker(
    [df_origin, df_destination],
    settings,
    input_table_aliases=["__ori", "_dest"],
    db_api=db_api,
)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

linker.training.estimate_parameters_using_expectation_maximisation(block_on("memo"))

session = linker.training.estimate_parameters_using_expectation_maximisation(block_on("amount"))

linker.visualisations.match_weights_chart()

df_predict = linker.inference.predict(threshold_match_probability=0.001)

linker.visualisations.comparison_viewer_dashboard(
    df_predict, "dashboards/comparison_viewer_transactions.html", overwrite=True
)
from IPython.display import IFrame

IFrame(
    src="./dashboards/comparison_viewer_transactions.html", width="100%", height=1200
)

pred_errors = linker.evaluation.prediction_errors_from_labels_column(
    "ground_truth", include_false_positives=True, include_false_negatives=False
)
linker.visualisations.waterfall_chart(pred_errors.as_record_dict(limit=5))

pred_errors = linker.evaluation.prediction_errors_from_labels_column(
    "ground_truth", include_false_positives=False, include_false_negatives=True
)
linker.visualisations.waterfall_chart(pred_errors.as_record_dict(limit=5))



Python code from ../docs/demos/examples/duckdb/deduplicate_50k_synthetic.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import splink_datasets

df = splink_datasets.historical_50k

df.head()

from splink import DuckDBAPI
from splink.exploratory import profile_columns

db_api = DuckDBAPI()
profile_columns(df, db_api, column_expressions=["first_name", "substr(surname,1,2)"])

from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules = [
    block_on("substr(first_name,1,3)", "substr(surname,1,4)"),
    block_on("surname", "dob"),
    block_on("first_name", "dob"),
    block_on("postcode_fake", "first_name"),
    block_on("postcode_fake", "surname"),
    block_on("dob", "birth_place"),
    block_on("substr(postcode_fake,1,3)", "dob"),
    block_on("substr(postcode_fake,1,3)", "first_name"),
    block_on("substr(postcode_fake,1,3)", "surname"),
    block_on("substr(first_name,1,2)", "substr(surname,1,2)", "substr(dob,1,4)"),
]

db_api = DuckDBAPI()

cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=df,
    blocking_rules=blocking_rules,
    db_api=db_api,
    link_type="dedupe_only",
)

import splink.comparison_library as cl

from splink import Linker, SettingsCreator

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.ForenameSurnameComparison(
            "first_name",
            "surname",
            forename_surname_concat_col_name="first_name_surname_concat",
        ),
        cl.DateOfBirthComparison(
            "dob", input_is_string=True
        ),
        cl.PostcodeComparison("postcode_fake"),
        cl.ExactMatch("birth_place").configure(term_frequency_adjustments=True),
        cl.ExactMatch("occupation").configure(term_frequency_adjustments=True),
    ],
    retain_intermediate_calculation_columns=True,
)
# Needed to apply term frequencies to first+surname comparison
df["first_name_surname_concat"] = df["first_name"] + " " + df["surname"]
linker = Linker(df, settings, db_api=db_api)

linker.training.estimate_probability_two_random_records_match(
    [
        "l.first_name = r.first_name and l.surname = r.surname and l.dob = r.dob",
        "substr(l.first_name,1,2) = substr(r.first_name,1,2) and l.surname = r.surname and substr(l.postcode_fake,1,2) = substr(r.postcode_fake,1,2)",
        "l.dob = r.dob and l.postcode_fake = r.postcode_fake",
    ],
    recall=0.6,
)

linker.training.estimate_u_using_random_sampling(max_pairs=5e6)

training_blocking_rule = block_on("first_name", "surname")
training_session_names = (
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule, estimate_without_term_frequencies=True
    )
)

training_blocking_rule = block_on("dob")
training_session_dob = (
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule, estimate_without_term_frequencies=True
    )
)

linker.visualisations.match_weights_chart()

linker.evaluation.unlinkables_chart()

df_predict = linker.inference.predict()
df_e = df_predict.as_pandas_dataframe(limit=5)
df_e

records_to_plot = df_e.to_dict(orient="records")
linker.visualisations.waterfall_chart(records_to_plot, filter_nulls=False)

clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predict, threshold_match_probability=0.95
)

from IPython.display import IFrame

linker.visualisations.cluster_studio_dashboard(
    df_predict,
    clusters,
    "dashboards/50k_cluster.html",
    sampling_method="by_cluster_size",
    overwrite=True,
)


IFrame(src="./dashboards/50k_cluster.html", width="100%", height=1200)

linker.evaluation.accuracy_analysis_from_labels_column(
    "cluster", output_type="accuracy", match_weight_round_to_nearest=0.02
)

records = linker.evaluation.prediction_errors_from_labels_column(
    "cluster",
    threshold_match_probability=0.999,
    include_false_negatives=False,
    include_false_positives=True,
).as_record_dict()
linker.visualisations.waterfall_chart(records)

# Some of the false negatives will be because they weren't detected by the blocking rules
records = linker.evaluation.prediction_errors_from_labels_column(
    "cluster",
    threshold_match_probability=0.5,
    include_false_negatives=True,
    include_false_positives=False,
).as_record_dict(limit=50)

linker.visualisations.waterfall_chart(records)



Python code from ../docs/demos/examples/duckdb/febrl4.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import splink_datasets

df_a = splink_datasets.febrl4a
df_b = splink_datasets.febrl4b


def prepare_data(data):
    data = data.rename(columns=lambda x: x.strip())
    data["cluster"] = data["rec_id"].apply(lambda x: "-".join(x.split("-")[:2]))
    data["date_of_birth"] = data["date_of_birth"].astype(str).str.strip()
    data["soc_sec_id"] = data["soc_sec_id"].astype(str).str.strip()
    data["postcode"] = data["postcode"].astype(str).str.strip()
    return data


dfs = [prepare_data(dataset) for dataset in [df_a, df_b]]

display(dfs[0].head(2))
display(dfs[1].head(2))

from splink import DuckDBAPI, Linker, SettingsCreator

basic_settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="link_only",
    # NB as we are linking one-one, we know the probability that a random pair will be a match
    # hence we could set:
    # "probability_two_random_records_match": 1/5000,
    # however we will not specify this here, as we will use this as a check that
    # our estimation procedure returns something sensible
)

linker = Linker(dfs, basic_settings, db_api=DuckDBAPI())

from splink.exploratory import completeness_chart

completeness_chart(dfs, db_api=DuckDBAPI())

from splink.exploratory import profile_columns

profile_columns(dfs, db_api=DuckDBAPI(), column_expressions=["given_name", "surname"])

from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules = [
    block_on("given_name", "surname"),
    # A blocking rule can also be an aribtrary SQL expression
    "l.given_name = r.surname and l.surname = r.given_name",
    block_on("date_of_birth"),
    block_on("soc_sec_id"),
    block_on("state", "address_1"),
    block_on("street_number", "address_1"),
    block_on("postcode"),
]


db_api = DuckDBAPI()
cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=dfs,
    blocking_rules=blocking_rules,
    db_api=db_api,
    link_type="link_only",
    unique_id_column_name="rec_id",
    source_dataset_column_name="source_dataset",
)

import splink.comparison_level_library as cll
import splink.comparison_library as cl


# the simple model only considers a few columns, and only two comparison levels for each
simple_model_settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="link_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.ExactMatch("given_name").configure(term_frequency_adjustments=True),
        cl.ExactMatch("surname").configure(term_frequency_adjustments=True),
        cl.ExactMatch("street_number").configure(term_frequency_adjustments=True),
    ],
    retain_intermediate_calculation_columns=True,
)

# the detailed model considers more columns, using the information we saw in the exploratory phase
# we also include further comparison levels to account for typos and other differences
detailed_model_settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="link_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.NameComparison("given_name").configure(term_frequency_adjustments=True),
        cl.NameComparison("surname").configure(term_frequency_adjustments=True),
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            datetime_format="%Y%m%d",
            invalid_dates_as_null=True,
        ),
        cl.DamerauLevenshteinAtThresholds("soc_sec_id", [1, 2]),
        cl.ExactMatch("street_number").configure(term_frequency_adjustments=True),
        cl.DamerauLevenshteinAtThresholds("postcode", [1, 2]).configure(
            term_frequency_adjustments=True
        ),
        # we don't consider further location columns as they will be strongly correlated with postcode
    ],
    retain_intermediate_calculation_columns=True,
)


linker_simple = Linker(dfs, simple_model_settings, db_api=DuckDBAPI())
linker_detailed = Linker(dfs, detailed_model_settings, db_api=DuckDBAPI())

deterministic_rules = [
    block_on("soc_sec_id"),
    block_on("given_name", "surname", "date_of_birth"),
]

linker_detailed.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.8
)

# We generally recommend setting max pairs higher (e.g. 1e7 or more)
# But this will run faster for the purpose of this demo
linker_detailed.training.estimate_u_using_random_sampling(max_pairs=1e6)

session_dob = (
    linker_detailed.training.estimate_parameters_using_expectation_maximisation(
        block_on("date_of_birth"), estimate_without_term_frequencies=True
    )
)
session_pc = (
    linker_detailed.training.estimate_parameters_using_expectation_maximisation(
        block_on("postcode"), estimate_without_term_frequencies=True
    )
)

session_dob.m_u_values_interactive_history_chart()

linker_detailed.visualisations.parameter_estimate_comparisons_chart()

linker_simple.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.8
)
linker_simple.training.estimate_u_using_random_sampling(max_pairs=1e7)
session_ssid = (
    linker_simple.training.estimate_parameters_using_expectation_maximisation(
        block_on("given_name"), estimate_without_term_frequencies=True
    )
)
session_pc = linker_simple.training.estimate_parameters_using_expectation_maximisation(
    block_on("street_number"), estimate_without_term_frequencies=True
)
linker_simple.visualisations.parameter_estimate_comparisons_chart()

# import json
# we can have a look at the full settings if we wish, including the values of our estimated parameters:
# print(json.dumps(linker_detailed._settings_obj.as_dict(), indent=2))
# we can also get a handy summary of of the model in an easily readable format if we wish:
# print(linker_detailed._settings_obj.human_readable_description)
# (we suppress output here for brevity)

linker_simple.visualisations.match_weights_chart()

linker_detailed.visualisations.match_weights_chart()

# linker_simple.m_u_parameters_chart()
linker_detailed.visualisations.m_u_parameters_chart()

linker_simple.evaluation.unlinkables_chart()

linker_detailed.evaluation.unlinkables_chart()

predictions = linker_detailed.inference.predict(threshold_match_probability=0.2)
df_predictions = predictions.as_pandas_dataframe()
df_predictions.head(5)

linker_detailed.evaluation.accuracy_analysis_from_labels_column(
    "cluster", output_type="accuracy"
)

clusters = linker_detailed.clustering.cluster_pairwise_predictions_at_threshold(
    predictions, threshold_match_probability=0.99
)
df_clusters = clusters.as_pandas_dataframe().sort_values("cluster_id")
df_clusters.groupby("cluster_id").size().value_counts()

df_predictions["cluster_l"] = df_predictions["rec_id_l"].apply(
    lambda x: "-".join(x.split("-")[:2])
)
df_predictions["cluster_r"] = df_predictions["rec_id_r"].apply(
    lambda x: "-".join(x.split("-")[:2])
)
df_true_links = df_predictions[
    df_predictions["cluster_l"] == df_predictions["cluster_r"]
].sort_values("match_probability")

records_to_view = 3
linker_detailed.visualisations.waterfall_chart(
    df_true_links.head(records_to_view).to_dict(orient="records")
)

df_non_links = df_predictions[
    df_predictions["cluster_l"] != df_predictions["cluster_r"]
].sort_values("match_probability", ascending=False)
linker_detailed.visualisations.waterfall_chart(
    df_non_links.head(records_to_view).to_dict(orient="records")
)

# we need to append a full name column to our source data frames
# so that we can use it for term frequency adjustments
dfs[0]["full_name"] = dfs[0]["given_name"] + "_" + dfs[0]["surname"]
dfs[1]["full_name"] = dfs[1]["given_name"] + "_" + dfs[1]["surname"]


extended_model_settings = {
    "unique_id_column_name": "rec_id",
    "link_type": "link_only",
    "blocking_rules_to_generate_predictions": blocking_rules,
    "comparisons": [
        {
            "output_column_name": "Full name",
            "comparison_levels": [
                {
                    "sql_condition": "(given_name_l IS NULL OR given_name_r IS NULL) and (surname_l IS NULL OR surname_r IS NULL)",
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                # full name match
                cll.ExactMatchLevel("full_name", term_frequency_adjustments=True),
                # typos - keep levels across full name rather than scoring separately
                cll.JaroWinklerLevel("full_name", 0.9),
                cll.JaroWinklerLevel("full_name", 0.7),
                # name switched
                cll.ColumnsReversedLevel("given_name", "surname"),
                # name switched + typo
                {
                    "sql_condition": "jaro_winkler_similarity(given_name_l, surname_r) + jaro_winkler_similarity(surname_l, given_name_r) >= 1.8",
                    "label_for_charts": "switched + jaro_winkler_similarity >= 1.8",
                },
                {
                    "sql_condition": "jaro_winkler_similarity(given_name_l, surname_r) + jaro_winkler_similarity(surname_l, given_name_r) >= 1.4",
                    "label_for_charts": "switched + jaro_winkler_similarity >= 1.4",
                },
                # single name match
                cll.ExactMatchLevel("given_name", term_frequency_adjustments=True),
                cll.ExactMatchLevel("surname", term_frequency_adjustments=True),
                # single name cross-match
                {
                    "sql_condition": "given_name_l = surname_r OR surname_l = given_name_r",
                    "label_for_charts": "single name cross-matches",
                },  # single name typos
                cll.JaroWinklerLevel("given_name", 0.9),
                cll.JaroWinklerLevel("surname", 0.9),
                # the rest
                cll.ElseLevel(),
            ],
        },
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            datetime_format="%Y%m%d",
            invalid_dates_as_null=True,
        ),
        {
            "output_column_name": "Social security ID",
            "comparison_levels": [
                cll.NullLevel("soc_sec_id"),
                cll.ExactMatchLevel("soc_sec_id", term_frequency_adjustments=True),
                cll.DamerauLevenshteinLevel("soc_sec_id", 1),
                cll.DamerauLevenshteinLevel("soc_sec_id", 2),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "Street number",
            "comparison_levels": [
                cll.NullLevel("street_number"),
                cll.ExactMatchLevel("street_number", term_frequency_adjustments=True),
                cll.DamerauLevenshteinLevel("street_number", 1),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "Postcode",
            "comparison_levels": [
                cll.NullLevel("postcode"),
                cll.ExactMatchLevel("postcode", term_frequency_adjustments=True),
                cll.DamerauLevenshteinLevel("postcode", 1),
                cll.DamerauLevenshteinLevel("postcode", 2),
                cll.ElseLevel(),
            ],
        },
        # we don't consider further location columns as they will be strongly correlated with postcode
    ],
    "retain_intermediate_calculation_columns": True,
}

# train
linker_advanced = Linker(dfs, extended_model_settings, db_api=DuckDBAPI())
linker_advanced.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.8
)
# We recommend increasing target rows to 1e8 improve accuracy for u
# values in full name comparison, as we have subdivided the data more finely

# Here, 1e7 for speed
linker_advanced.training.estimate_u_using_random_sampling(max_pairs=1e7)

session_dob = (
    linker_advanced.training.estimate_parameters_using_expectation_maximisation(
        "l.date_of_birth = r.date_of_birth", estimate_without_term_frequencies=True
    )
)

session_pc = (
    linker_advanced.training.estimate_parameters_using_expectation_maximisation(
        "l.postcode = r.postcode", estimate_without_term_frequencies=True
    )
)

linker_advanced.visualisations.parameter_estimate_comparisons_chart()

linker_advanced.visualisations.match_weights_chart()

predictions_adv = linker_advanced.inference.predict()
df_predictions_adv = predictions_adv.as_pandas_dataframe()
clusters_adv = linker_advanced.clustering.cluster_pairwise_predictions_at_threshold(
    predictions_adv, threshold_match_probability=0.99
)
df_clusters_adv = clusters_adv.as_pandas_dataframe().sort_values("cluster_id")
df_clusters_adv.groupby("cluster_id").size().value_counts()



Python code from ../docs/demos/examples/duckdb/cookbook.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

import logging
logging.getLogger("splink").setLevel(logging.ERROR)


import pandas as pd

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on


data = [
    {"unique_id": 1, "first_name": "John", "postcode": ["A", "B"]},
    {"unique_id": 2, "first_name": "John", "postcode": ["B"]},
    {"unique_id": 3, "first_name": "John", "postcode": ["A"]},
    {"unique_id": 4, "first_name": "John", "postcode": ["A", "B"]},
    {"unique_id": 5, "first_name": "John", "postcode": ["C"]},
]

df = pd.DataFrame(data)

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
    ],
    comparisons=[
        cl.ArrayIntersectAtSizes("postcode", [2, 1]),
        cl.ExactMatch("first_name"),
    ]
)


linker = Linker(df, settings, DuckDBAPI(), set_up_basic_logging=False)

linker.inference.predict().as_pandas_dataframe()

import pandas as pd

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on


data = [
    {"unique_id": 1, "first_name": "John", "postcode": ["A", "B"]},
    {"unique_id": 2, "first_name": "John", "postcode": ["B"]},
    {"unique_id": 3, "first_name": "John", "postcode": ["C"]},

]

df = pd.DataFrame(data)

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("postcode", arrays_to_explode=["postcode"]),
    ],
    comparisons=[
        cl.ArrayIntersectAtSizes("postcode", [2, 1]),
        cl.ExactMatch("first_name"),
    ]
)


linker = Linker(df, settings, DuckDBAPI(), set_up_basic_logging=False)

linker.inference.predict().as_pandas_dataframe()


import duckdb
import tempfile
import os

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

# Create a parquet file on disk to demontrate native DuckDB parquet reading
df = splink_datasets.fake_1000
temp_file = tempfile.NamedTemporaryFile(delete=True, suffix=".parquet")
temp_file_path = temp_file.name
df.to_parquet(temp_file_path)

# Example would start here if you already had a parquet file
duckdb_df = duckdb.read_parquet(temp_file_path)

db_api = DuckDBAPI(":default:")
settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.NameComparison("first_name"),
        cl.JaroAtThresholds("surname"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name", "dob"),
        block_on("surname"),
    ],
)

linker = Linker(df, settings, db_api, set_up_basic_logging=False)

result = linker.inference.predict().as_duckdbpyrelation()

# Since result is a DuckDBPyRelation, we can use all the usual DuckDB API
# functions on it.

# For example, we can use the `sort` function to sort the results,
# or could use result.to_parquet() to write to a parquet file.
result.sort("match_weight")


import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets


db_api = DuckDBAPI()

first_name_comparison = cl.CustomComparison(
    comparison_levels=[
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name").configure(
            m_probability=0.9999,
            fix_m_probability=True,
            u_probability=0.7,
            fix_u_probability=True,
        ),
        cll.ElseLevel(),
    ]
)
settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        first_name_comparison,
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("dob"),
    ],
    additional_columns_to_retain=["cluster"],
)

df = splink_datasets.fake_1000
linker = Linker(df, settings, db_api, set_up_basic_logging=False)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

linker.visualisations.m_u_parameters_chart()

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets
from splink.datasets import splink_dataset_labels

labels = splink_dataset_labels.fake_1000_labels

db_api = DuckDBAPI()


settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("dob"),
    ],
)
df = splink_datasets.fake_1000
linker = Linker(df, settings, db_api, set_up_basic_logging=False)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))


surname_comparison = linker._settings_obj._get_comparison_by_output_column_name(
    "surname"
)
else_comparison_level = (
    surname_comparison._get_comparison_level_by_comparison_vector_value(0)
)
else_comparison_level._m_probability = 0.1


linker.visualisations.m_u_parameters_chart()

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

db_api = DuckDBAPI()

df = splink_datasets.fake_1000

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.ExactMatch("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    max_iterations=2,
)

linker = Linker(df, settings, db_api, set_up_basic_logging=False)

linker.training.estimate_probability_two_random_records_match(
    [block_on("first_name", "surname")], recall=0.7
)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

pairwise_predictions = linker.inference.predict(threshold_match_weight=-10)

first_unique_id = df.iloc[0].unique_id
linker.evaluation.labelling_tool_for_specific_record(unique_id=first_unique_id, overwrite=True)




Python code from ../docs/demos/examples/spark/deduplicate_1k_synthetic.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink
# !pip install pyspark

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from splink.backends.spark import similarity_jar_location

conf = SparkConf()
# This parallelism setting is only suitable for a small toy example
conf.set("spark.driver.memory", "12g")
conf.set("spark.default.parallelism", "8")
conf.set("spark.sql.codegen.wholeStage", "false")


# Add custom similarity functions, which are bundled with Splink
# documented here: https://github.com/moj-analytical-services/splink_scalaudfs
path = similarity_jar_location()
conf.set("spark.jars", path)

sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)
spark.sparkContext.setCheckpointDir("./tmp_checkpoints")

# Disable warnings for pyspark - you don't need to include this
import warnings

spark.sparkContext.setLogLevel("ERROR")
warnings.simplefilter("ignore", UserWarning)

from splink import splink_datasets

pandas_df = splink_datasets.fake_1000

df = spark.createDataFrame(pandas_df)

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, SparkAPI, block_on

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.LevenshteinAtThresholds(
            "dob"
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        "l.surname = r.surname",  # alternatively, you can write BRs in their SQL form
    ],
    retain_intermediate_calculation_columns=True,
    em_convergence=0.01,
)

linker = Linker(df, settings, db_api=SparkAPI(spark_session=spark))
deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob, l.dob) <= 1",
    "l.surname = r.surname and levenshtein(r.dob, l.dob) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    "l.email = r.email",
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.6)

linker.training.estimate_u_using_random_sampling(max_pairs=5e5)

training_blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
training_session_fname_sname = (
    linker.training.estimate_parameters_using_expectation_maximisation(training_blocking_rule)
)

training_blocking_rule = "l.dob = r.dob"
training_session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    training_blocking_rule
)

results = linker.inference.predict(threshold_match_probability=0.9)

spark_df = results.as_spark_dataframe().show()



Python code from ../docs/demos/tutorials/03_Blocking.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import DuckDBAPI, block_on, splink_datasets

df = splink_datasets.fake_1000

from splink.blocking_analysis import count_comparisons_from_blocking_rule

db_api = DuckDBAPI()

br = block_on("substr(first_name, 1,1)", "surname")

counts = count_comparisons_from_blocking_rule(
    table_or_tables=df,
    blocking_rule=br,
    link_type="dedupe_only",
    db_api=db_api,
)

counts

br = "l.first_name = r.first_name and levenshtein(l.surname, r.surname) < 2"

counts = count_comparisons_from_blocking_rule(
    table_or_tables=df,
    blocking_rule= br,
    link_type="dedupe_only",
    db_api=db_api,
)
counts

from splink.blocking_analysis import n_largest_blocks

result = n_largest_blocks(    table_or_tables=df,
    blocking_rule= block_on("city", "first_name"),
    link_type="dedupe_only",
    db_api=db_api,
    n_largest=3
    )

result.as_pandas_dataframe()

from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules_for_analysis = [
    block_on("substr(first_name, 1,1)", "surname"),
    block_on("surname"),
    block_on("email"),
    block_on("city", "first_name"),
    "l.first_name = r.first_name and levenshtein(l.surname, r.surname) < 2",
]


cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=df,
    blocking_rules=blocking_rules_for_analysis,
    db_api=db_api,
    link_type="dedupe_only",
)

from splink.exploratory import profile_columns

profile_columns(df, column_expressions=["city || left(first_name,1)"], db_api=db_api)



Python code from ../docs/demos/tutorials/07_Evaluation.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

# Rerun our predictions to we're ready to view the charts
import pandas as pd

from splink import DuckDBAPI, Linker, splink_datasets

pd.options.display.max_columns = 1000

db_api = DuckDBAPI()
df = splink_datasets.fake_1000

import json
import urllib

from splink import block_on

url = "https://raw.githubusercontent.com/moj-analytical-services/splink/847e32508b1a9cdd7bcd2ca6c0a74e547fb69865/docs/demos/demo_settings/saved_model_from_demo.json"

with urllib.request.urlopen(url) as u:
    settings = json.loads(u.read().decode())

# The data quality is very poor in this dataset, so we need looser blocking rules
# to achieve decent recall
settings["blocking_rules_to_generate_predictions"] = [
    block_on("first_name"),
    block_on("city"),
    block_on("email"),
    block_on("dob"),
]

linker = Linker(df, settings, db_api=DuckDBAPI())
df_predictions = linker.inference.predict(threshold_match_probability=0.01)

from splink.datasets import splink_dataset_labels

df_labels = splink_dataset_labels.fake_1000_labels
labels_table = linker.table_management.register_labels_table(df_labels)
df_labels.head(5)

splink_df = linker.evaluation.prediction_errors_from_labels_table(
    labels_table, include_false_negatives=True, include_false_positives=False
)
false_negatives = splink_df.as_record_dict(limit=5)
linker.visualisations.waterfall_chart(false_negatives)

# Note I've picked a threshold match probability of 0.01 here because otherwise
# in this simple example there are no false positives
splink_df = linker.evaluation.prediction_errors_from_labels_table(
    labels_table, include_false_negatives=False, include_false_positives=True, threshold_match_probability=0.01
)
false_postives = splink_df.as_record_dict(limit=5)
linker.visualisations.waterfall_chart(false_postives)

linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="threshold_selection", add_metrics=["f1"]
)

linker.evaluation.accuracy_analysis_from_labels_table(labels_table, output_type="roc")

roc_table = linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="table"
)
roc_table.as_pandas_dataframe(limit=5)

linker.evaluation.unlinkables_chart()



Python code from ../docs/demos/tutorials/00_Tutorial_Introduction.ipynb:


Python code from ../docs/demos/tutorials/02_Exploratory_analysis.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import  splink_datasets

df = splink_datasets.fake_1000
df = df.drop(columns=["cluster"])
df.head(5)

from splink.exploratory import completeness_chart
from splink import DuckDBAPI
db_api = DuckDBAPI()
completeness_chart(df, db_api=db_api)

from splink.exploratory import profile_columns

profile_columns(df, db_api=DuckDBAPI(), top_n=10, bottom_n=5)



Python code from ../docs/demos/tutorials/01_Prerequisites.ipynb:


Python code from ../docs/demos/tutorials/04_Estimating_model_parameters.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

# Begin by reading in the tutorial data again
from splink import splink_datasets

df = splink_datasets.fake_1000

import splink.comparison_library as cl

city_comparison = cl.LevenshteinAtThresholds("city", 2)
print(city_comparison.get_comparison("duckdb").human_readable_description)

email_comparison = cl.EmailComparison("email")
print(email_comparison.get_comparison("duckdb").human_readable_description)

from splink import Linker, SettingsCreator, block_on, DuckDBAPI

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.LevenshteinAtThresholds("dob", 1),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name", "city"),
        block_on("surname"),

    ],
    retain_intermediate_calculation_columns=True,
)

linker = Linker(df, settings, db_api=DuckDBAPI())

deterministic_rules = [
    block_on("first_name", "dob"),
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    block_on("email")
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

training_blocking_rule = block_on("first_name", "surname")
training_session_fname_sname = (
    linker.training.estimate_parameters_using_expectation_maximisation(training_blocking_rule)
)

training_blocking_rule = block_on("dob")
training_session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    training_blocking_rule
)

linker.visualisations.match_weights_chart()

linker.visualisations.m_u_parameters_chart()

linker.visualisations.parameter_estimate_comparisons_chart()

settings = linker.misc.save_model_to_json(
    "../demo_settings/saved_model_from_demo.json", overwrite=True
)

linker.evaluation.unlinkables_chart()



Python code from ../docs/demos/tutorials/05_Predicting_results.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

from splink import Linker, DuckDBAPI, splink_datasets

import pandas as pd

pd.options.display.max_columns = 1000

db_api = DuckDBAPI()
df = splink_datasets.fake_1000

import json
import urllib

url = "https://raw.githubusercontent.com/moj-analytical-services/splink/847e32508b1a9cdd7bcd2ca6c0a74e547fb69865/docs/demos/demo_settings/saved_model_from_demo.json"

with urllib.request.urlopen(url) as u:
    settings = json.loads(u.read().decode())


linker = Linker(df, settings, db_api=DuckDBAPI())

df_predictions = linker.inference.predict(threshold_match_probability=0.2)
df_predictions.as_pandas_dataframe(limit=5)

clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predictions, threshold_match_probability=0.5
)
clusters.as_pandas_dataframe(limit=10)

sql = f"""
select *
from {df_predictions.physical_name}
limit 2
"""
linker.misc.query_sql(sql)



Python code from ../docs/demos/tutorials/06_Visualising_predictions.ipynb:
# Uncomment and run this cell if you're running in Google Colab.
# !pip install splink

# Rerun our predictions to we're ready to view the charts
from splink import Linker, DuckDBAPI, splink_datasets

import pandas as pd

pd.options.display.max_columns = 1000

db_api = DuckDBAPI()
df = splink_datasets.fake_1000

import json
import urllib

url = "https://raw.githubusercontent.com/moj-analytical-services/splink/847e32508b1a9cdd7bcd2ca6c0a74e547fb69865/docs/demos/demo_settings/saved_model_from_demo.json"

with urllib.request.urlopen(url) as u:
    settings = json.loads(u.read().decode())


linker = Linker(df, settings, db_api=DuckDBAPI())
df_predictions = linker.inference.predict(threshold_match_probability=0.2)

records_to_view = df_predictions.as_record_dict(limit=5)
linker.visualisations.waterfall_chart(records_to_view, filter_nulls=False)

linker.visualisations.comparison_viewer_dashboard(df_predictions, "scv.html", overwrite=True)

# You can view the scv.html file in your browser, or inline in a notbook as follows
from IPython.display import IFrame

IFrame(src="./scv.html", width="100%", height=1200)

df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predictions, threshold_match_probability=0.5
)

linker.visualisations.cluster_studio_dashboard(
    df_predictions,
    df_clusters,
    "cluster_studio.html",
    sampling_method="by_cluster_size",
    overwrite=True,
)

# You can view the scv.html file in your browser, or inline in a notbook as follows
from IPython.display import IFrame

IFrame(src="./cluster_studio.html", width="100%", height=1000)





Contents of /docs/index.md:
---
hide:
  - navigation
  - toc
---

<p align="center">
<img src="https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png" alt="Splink: data linkage at scale. (Splink logo)." style="max-width: 500px;">
</p>

# Fast, accurate and scalable probabilistic data linkage

Splink is a Python package for probabilistic record linkage (entity resolution) that allows you to deduplicate and link records from datasets without unique identifiers.

[Get Started with Splink](./getting_started.md){ .md-button .md-button--primary }

<hr>

## Key Features

⚡ **Speed:** Capable of linking a million records on a laptop in approximately one minute.<br>
🎯 **Accuracy:** Full support for term frequency adjustments and user-defined fuzzy matching logic.<br>
🌐 **Scalability:** Execute linkage jobs in Python (using DuckDB) or big-data backends like AWS Athena or Spark for 100+ million records.<br>
🎓 **Unsupervised Learning:** No training data is required, as models can be trained using an unsupervised approach.<br>
📊 **Interactive Outputs:** Provides a wide range of interactive outputs to help users understand their model and diagnose linkage problems.<br>

Splink's core linkage algorithm is based on Fellegi-Sunter's model of record linkage, with various customizations to improve accuracy.

## What does Splink do?

Consider the following records that lack a unique person identifier:

![tables showing what Splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_1.drawio.png)

Splink predicts which rows link together:

![tables showing what Splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_2.drawio.png)

and clusters these links to produce an estimated person ID:

![tables showing what Splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_3.drawio.png)

## What data does Splink work best with?

Before using Splink, input data should be standardised, with consistent column names and formatting (e.g., lowercased, punctuation cleaned up, etc.).

Splink performs best with input data containing **multiple** columns that are **not highly correlated**. For instance, if the entity type is persons, you may have columns for full name, date of birth, and city. If the entity type is companies, you could have columns for name, turnover, sector, and telephone number.

High correlation occurs when the value of a column is highly constrained (predictable) from the value of another column. For example, a 'city' field is almost perfectly correlated with 'postcode'. Gender is highly correlated with 'first name'. Correlation is particularly problematic if **all** of your input columns are highly correlated.

Splink is not designed for linking a single column containing a 'bag of words'. For example, a table with a single 'company name' column, and no other details.

## Support

If after reading the documentatation you still have questions, please feel free to post on our [discussion forum](https://github.com/moj-analytical-services/splink/discussions).

## Use Cases

Here is a list of some of our known users and their use cases:

=== "Public Sector (UK)"

	- [Ministry of Justice](https://www.gov.uk/government/organisations/ministry-of-justice) created [linked datasets (combining courts, prisons and probation data)](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) for use by researchers as part of the [Data First programme](https://www.gov.uk/guidance/ministry-of-justice-data-first)
	- [Office for National Statistics](https://www.ons.gov.uk/)'s [Business Index](https://unece.org/sites/default/files/2023-04/ML2023_S1_UK_Breton_A.pdf) (formerly the Inter Departmental Business Register), [Demographic Index](https://uksa.statisticsauthority.gov.uk/wp-content/uploads/2023/02/EAP182-Quality-work-for-Demographic-Index-MDQA.pdf) and the [2021 Census](https://github.com/Data-Linkage/Splink-census-linkage/blob/main/SplinkCaseStudy.pdf).  See also [this article](https://www.government-transformation.com/data/interview-modernizing-public-sector-insight-through-automated-linkage).
	- [Lewisham Council](https://lewisham.gov.uk/) (London) [identified and auto-enrolled over 500 additional eligible families](https://lewisham.gov.uk/articles/news/extra-funding-for-lewisham-schools-in-pilot-data-project) to receive Free School Meals
	- [London Office of Technology and Innovation](https://loti.london/) created a dashboard to help [better measure and reduce rough sleeping](https://loti.london/projects/rough-sleeping-insights-project/) across London
	- [Competition and Markets Authority](https://www.gov.uk/government/organisations/competition-and-markets-authority) identified ['Persons with Significant Control' and estimated ownership groups](https://assets.publishing.service.gov.uk/media/626ab6c4d3bf7f0e7f9d5a9b/220426_Annex_-State_of_Competition_Appendices_FINAL.pdf) across companies
	- [Office for Health Improvement and Disparities](https://www.gov.uk/government/organisations/office-for-health-improvement-and-disparities) linked Health and Justice data to [assess the pathways between probation and specialist alcohol and drug treatment services](https://www.gov.uk/government/statistics/pathways-between-probation-and-addiction-treatment-in-england#:~:text=Details,of%20Health%20and%20Social%20Care) as part of the [Better Outcomes through Linked Data programme](https://www.gov.uk/government/publications/ministry-of-justice-better-outcomes-through-linked-data-bold)
	- [Ministry of Defence](https://www.gov.uk/government/organisations/ministry-of-defence) recently launched their [Veteran's Card system](https://www.gov.uk/government/news/hm-armed-forces-veteran-cards-will-officially-launch-in-the-new-year-following-a-successful-assessment-from-the-central-digital-and-data-office) which uses Splink to verify applicants against historic records. This project was shortlisted for the [Civil Service Awards](https://www.civilserviceawards.com/creative-solutions-award/)
 	- [Gateshead Council](https://www.gateshead.gov.uk/), in partnership with the [National Innovation Centre for Data](https://www.nicd.org.uk/) are creating a [single view of debt](https://nicd.org.uk/knowledge-hub/an-end-to-end-guide-to-overcoming-unique-identifier-challenges-with-splink)
	- The Defense Health Agency (US Department of Defense) used Splink to identify duplicated hospital records across over 200 million data points in the military hospital data system


=== "Public Sector (International)"

	- The German Federal Statistical Office ([Destatis](https://www.destatis.de/EN/Home/_node.html)) uses Splink to conduct projects in linking register-based census data.
	- The [European Medicines Agency](https://www.ema.europa.eu/en/homepage) uses Splink to detect duplicate adverse event reports for veterinary medicines
	- [Chilean Ministry of Health](https://www.gob.cl/en/ministries/ministry-of-health/) and [University College London](https://www.ucl.ac.uk/) have [assessed the access to immunisation programs among the migrant population](https://ijpds.org/article/view/2348)
	- [Florida Cancer Registry](https://www.floridahealth.gov/diseases-and-conditions/cancer/cancer-registry/index.html), published a [feasibility study](https://scholar.googleusercontent.com/scholar?q=cache:sADwxy-D75IJ:scholar.google.com/+splink+florida&hl=en&as_sdt=0,5) which showed Splink was faster and more accurate than alternatives
	- [Catalyst Cooperative](https://catalyst.coop)'s [Public Utility Data Liberation Project](https://github.com/catalyst-cooperative/pudl) links public financial and operational data from electric utilities for use by US climate advocates, policymakers, and researchers seeking to accelerate the transition away from fossil fuels.

=== "Academia"

	- [Stanford University](https://www.stanford.edu/) investigated the impact of [receiving government assistance has on political attitudes](https://www.cambridge.org/core/journals/american-political-science-review/article/abs/does-receiving-government-assistance-shape-political-attitudes-evidence-from-agricultural-producers/39552BC5A496EAB6CB484FCA51C6AF21)
	- [Bern University](https://arbor.bfh.ch/) researched how [Active Learning can be applied to Biomedical Record Linkage](https://ebooks.iospress.nl/doi/10.3233/SHTI230545)

=== "Other"
	- [Marie Curie](https://podcasts.apple.com/gb/podcast/unlocking-data-at-marie-curie/id1724979056?i=1000649964922) have used Splink to build a single customer view on fundraising data which has been a "huge success [...] the tooling is just so much better. [...] The power of being able to select, plug in, configure and train a tool versus writing code. It's just mind boggling actually."  Amongst other benefits, the system is expected to "dramatically reduce manual reporting efforts previously required". See also the blog post [here](https://esynergy.co.uk/our-work/marie-curie/).
 	- [Club Brugge](https://www.clubbrugge.be/en) uses Splink to link football players from different data providers to their own database, simplifying and reducing the need for manual linkage labor.
	- [GN Group](https://www.gn.com/) use Splink to deduplicate large volumes of customer records

Sadly, we don't hear about the majority of our users or what they are working on. If you have a use case and it is not shown here please [add it to the list](https://github.com/moj-analytical-services/splink/edit/master/docs/index.md)!

## Awards

🥈 Civil Service Awards 2023: Best Use of Data, Science, and Technology - [Runner up](https://www.civilserviceawards.com/best-use-of-data-science-and-technology-award-2/)

🥇 Analysis in Government Awards 2022: People's Choice Award - [Winner](https://analysisfunction.civilservice.gov.uk/news/announcing-the-winner-of-the-first-analysis-in-government-peoples-choice-award/)

🥈 Analysis in Government Awards 2022: Innovative Methods - [Runner up](https://twitter.com/gov_analysis/status/1616073633692274689?s=20&t=6TQyNLJRjnhsfJy28Zd6UQ)

🥇 Analysis in Government Awards 2020: Innovative Methods - [Winner](https://www.gov.uk/government/news/launch-of-the-analysis-in-government-awards)

🥇 Ministry of Justice Data and Analytical Services Directorate (DASD) Awards 2020: Innovation and Impact - Winner


## Citation

If you use Splink in your research, we'd be grateful for a citation as follows:

```BibTeX
@article{Linacre_Lindsay_Manassis_Slade_Hepworth_2022,
	title        = {Splink: Free software for probabilistic record linkage at scale.},
	author       = {Linacre, Robin and Lindsay, Sam and Manassis, Theodore and Slade, Zoe and Hepworth, Tom and Kennedy, Ross and Bond, Andrew},
	year         = 2022,
	month        = {Aug.},
	journal      = {International Journal of Population Data Science},
	volume       = 7,
	number       = 3,
	doi          = {10.23889/ijpds.v7i3.1794},
	url          = {https://ijpds.org/article/view/1794},
}
```

## Acknowledgements

We are very grateful to [ADR UK](https://www.adruk.org/) (Administrative Data Research UK) for providing the initial funding for this work as part of the [Data First](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) project.

We are extremely grateful to professors Katie Harron, James Doidge and Peter Christen for their expert advice and guidance in the development of Splink. We are also very grateful to colleagues at the UK's Office for National Statistics for their expert advice and peer review of this work. Any errors remain our own.




Contents of /docs/getting_started.md:
---
hide:
  - navigation
---

# Getting Started

## :material-download: Install
Splink supports python 3.8+.

To obtain the latest released version of Splink you can install from PyPI using pip:
```shell
pip install splink
```

or if you prefer, you can instead install Splink using conda:
```shell
conda install -c conda-forge splink
```

??? "Backend Specific Installs"
    ### Backend Specific Installs
    From Splink v3.9.7, packages required by specific Splink backends can be optionally installed by adding the `[<backend>]` suffix to the end of your pip install.

    **Note** that SQLite and DuckDB come packaged with Splink and do not need to be optionally installed.

    The following backends are supported:

    === ":simple-apachespark: Spark"
        ```sh
        pip install 'splink[spark]'
        ```

    === ":simple-amazonaws: Athena"
        ```sh
        pip install 'splink[athena]'
        ```

    === ":simple-postgresql: PostgreSQL"
        ```sh
        pip install 'splink[postgres]'
        ```



## :rocket: Quickstart

To get a basic Splink model up and running, use the following code. It demonstrates how to:

1. Estimate the parameters of a deduplication model
2. Use the parameter estimates to identify duplicate records
3. Use clustering to generate an estimated unique person ID.

???+ note "Simple Splink Model Example"
    ```py
    import splink.comparison_library as cl
    from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

    db_api = DuckDBAPI()

    df = splink_datasets.fake_1000

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.NameComparison("first_name"),
            cl.JaroAtThresholds("surname"),
            cl.DateOfBirthComparison(
                "dob",
                input_is_string=True,
            ),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.EmailComparison("email"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name", "dob"),
            block_on("surname"),
        ]
    )

    linker = Linker(df, settings, db_api)

    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name", "surname")
    )

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("email"))

    pairwise_predictions = linker.inference.predict(threshold_match_weight=-5)

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        pairwise_predictions, 0.95
    )

    df_clusters = clusters.as_pandas_dataframe(limit=5)
    ```

If you're using an LLM to suggest Splink code, see [here](./topic_guides/llms/prompting_llms.md) for suggested prompts and context.

## Tutorials

You can learn more about Splink in the step-by-step [tutorial](./demos/tutorials/00_Tutorial_Introduction.ipynb). Each has a corresponding Google Colab link to run the notebook in your browser.

## Example Notebooks

You can see end-to-end example of several use cases in the [example notebooks](./demos/examples/examples_index.md). Each has a corresponding Google Colab link to run the notebook in your browser.

## Getting help

If after reading the documentatation you still have questions, please feel free to post on our [discussion forum](https://github.com/moj-analytical-services/splink/discussions).



Contents of /docs/demos/tutorials/08_building_your_own_model.md:

# Next steps: Tips for Building your own model

Now that you've completed the tutorial, this page summarises some recommendations for how to approach building your own Splink models.

These recommendations should help you create an accurate model as quickly as possible.  They're particularly applicable if you're working with large datasets, where you can get slowed down by long processing times.

In a nutshell, we recommend beginning with a small sample and a basic model, then iteratively adding complexity to resolve issues and improve performance.

## General workflow

- **For large datasets, start by linking a small non-random sample of records**. Building a model is an iterative process of writing data cleaning code, training models, finding issues, and circling back to fix them. You don't want long processing times slowing down this iteration cycle.

    Most of your code can be developed against a small sample of records, and only once that's working, re-run everything on the full dataset.

    You need a **non-random** sample of perhaps about 10,000 records. The same must be  non-random because it must retain lots of matches - for instance, retain all people aged over 70, or all people with a first name starting with the characters `pa`.  You should aim to be able to run your full training and prediction script in less than a minute.

    Remember to set a lower value (say `1e6`) of the `target_rows` when calling `estimate_u_using_random_sampling()` during this iteration process, but then increase in the final full-dataset run to a much higher value, maybe `1e8`, since large value of `target_rows` can cause long processing times even on relatively small datasets.

- **Start with a simple model**.  It's often tempting to start by designing a complex model, with many granular comparison levels in an attempt to reflect the real world closely.

    Instead, we recommend starting with with a simple, rough and ready model where most comparisons have 2-3 levels (exact match, possibly a fuzzy level, and everything else).  The idea is to get to the point of looking at prediction results as quickly as possible using e.g. the comparison viewer.  You can then start to look for where your simple model is getting it wrong, and use that as the basis for improving your model, and iterating until you're seeing good results.

## Blocking rules for prediction

- **Analyse the number of comparisons before running predict**.  Use the tools in `splink.blocking_analysis` to validate that your rules aren't going to create a vast number of comparisons before asking Splink to create those comparisons.

- **Many strict `blocking_rules_for_prediction` are generally better than few loose rules.**  Whilst individually, strict blocking rules are likely to exclude many true matches, between them it should be implausible that a truly matching record 'falls through' all the rules.  Many strict rules often result in far fewer overall comparisons and a small number of loose rules.  In practice, many of our real-life models have between about 10-15 `blocking_rules_for_prediction`.


## EM trainining

- **Predictions usually aren't very sensitive to `m` probabilities being a bit wrong**.  The hardest model parameters to estimate are the `m` probabilities.  It's fairly common for Expectation Maximisation to yield 'bad' (implausble) values.  Luckily, the accuracy of your model is usually not particularly sensitive to the `m` probabilities - it usually the `u` probabilities driving the biggest match weights.  If you're having problems, consider fixing some `m` probabilities by expert judgement - see [here](https://github.com/moj-analytical-services/splink/pull/2379) for how.

- **Convergence problems are often indicative of the need for further data cleaning**.  Whilst predictions often aren't terribly sensitive to `m` probabilities, question why the estimation procedue is producing bad parameter estimates.  To do this, it's often enough to look at a variety of predictions to see if you can spot edge cases where the model is not doing what's expected.  For instance, we may find matches where the first name is `Mr`.  By fixing this and reestimating, the parameter estimates often make more sense.

- **Blocking rules for EM training do not need high recall**.  The purpose of blocking rules for EM training is to find a subset of records which include a reasonably balanced mix of matches and non matches.  There is no requirement that these records contain all, or even most of the matches.  For more see [here](https://moj-analytical-services.github.io/splink/topic_guides/blocking/model_training.html)  To double check that parameter estimates are a result of a biased sample of matches, you can use `linker.visualisations.parameter_estimate_comparisons_chart`.

## Working with large datasets

To optimise memory usage and performance:

- **Avoid pandas for input/output** Whilst Splink supports inputs as pandas dataframes, and you can convert results to pandas using `.as_pandas_dataframe()`, we recommend against this for large datasets.  For large datasets, use the concept of a dataframe that's native to your database backend.  For example, if you're using Spark, it's best to read your files using Spark and pass Spark dataframes into Splink, and save any outputs using `splink_dataframe.as_spark_dataframe`.  With duckdb use the inbuilt duckdb csv/parquet reader, and output via `splinkdataframe.as_duckdbpyrelation`.

- **Avoid pandas for data cleaning**.  You will generally get substantially better performance by performing data cleaning in SQL using your chosen backend rather than using pandas.

- **Turn off intermediate columns when calling `predict()`**.  Whilst during the model development phase, it is useful to set `retain_intermediate_calculation_columns=True` and
    `retain_intermediate_calculation_columns_for_prediction=True` in your settings, you should generally turn these off when calling `predict()`.  This will result in a much smaller table as your result set.  If you want waterfall charts for individual pairs, you can use [`linker.inference.compare_two_records`](../../api_docs/inference.md)





Blog Articles:


Article from https://www.robinlinacre.com/intro_to_probabilistic_linkage/:
An Interactive Introduction to Record Linkage (Data Deduplication) in the Fellegi-Sunter framework>robinlinacreOriginally posted:2021-05-20.Last updated: 2023-09-12.Live edit this notebookhere.This is part1of thetutorialNext article →#An Interactive Introduction to the Fellegi-Sunter Model for Data Linkage/Deduplication#AimsThis is part one of a series of interactive articles that aim to provide an introduction to the theory of probabilistic record linkage and deduplication.In this article I provide a high-level introduction to theFellegi-Sunterframework and an interactive example of a linkage model.Subsequent articles explore the theory in more depth.These materials align closely to the probabilistic model used bySplink, a free software package for record linkage at scale.These articles cover the theory only.  For practical model building using Splink, seethe tutorial in the Splink docs.#What is probabilistic record linkage?Probablistic record linkage is a technique used to link together records that lack unique identifiers.In the absence of a unique identifier such as a National Insurance number, we can use a combination of individually non-unique variables such as name, gender and date of birth to identify individuals.Record linkage can be done within datasets (deduplication), between datasets (linkage), or both1.Linkage is 'probabilistic' in the sense that it subject to uncertainty and relies on the balance of evidence. For instance, in a large dataset, observing that two records match on the full nameJohn Smithprovides some evidence that these two records may refer to the same person, but this evidence is inconclusive because it's possible there are two differentJohn Smiths.More broadly,  it is often impossible to classify pairs of records as matches or non-matches beyond any doubt. Instead, the aim of probabilisitic record linkage is to quantify the probability that a pair of records refer to the same entity by considering evidence in favour and against a match and weighting it appropriately.The most common type of probabilistic record linkage model is called the Fellegi-Sunter model.We start with aprior, which represents the probability that two records drawn at random are a match. We then compare the two records, increasing the match probability where information in the record matches, and decreasing it when information differs.The amount we increase and decrease the match probability is determined by the 'partial_match_weights' of the model.For example, a match on postcode gives us more evidence in favour of a match on gender, since the latter is much more likely to occur by chance.The final prediction is a simple calculation:  we sum uppartial_match_weights to compute a final match weight, which is then converted into a probability.#ExampleLet's take a look at an example of a simple Fellegi-Sunter model to calculate match probability interactively. This model will compare the two records in the table, and assess whether they refer to the same person, or different people.You may edit the values in the table to see how the match probability changes.We can decompose this calculation into the sum of thepartial_match_weights using a waterfall chart, which is read from left to right. We start with the prior, and take each column into account in turn.  The size of the bar corresponds to thepartial_match_weight.You can hover over the bars to see how the probability changes as each subsequent field is taken into account.The final estimated match probability is shown in the rightmost bar.  Note that the y axis on the right converts match weight into probability.In the next article, we will look at partial match weights in great depth.#FootnotesRecord linkage and deduplication are equivalent problems.  The only difference is that linkage involves finding matching entities across datasets and deduplication involves finding matches within datasets.↩Probabilistic Linkage Tutorial Navigation:An Interactive Introduction to Record Linkage (Data Deduplication) in the Fellegi-Sunter frameworkPartial match weightsm and u values in the Fellegi-Sunter modelThe mathematics of the Fellegi Sunter modelComputing the Fellegi Sunter modelWhy Probabilistic Linkage is More Accurate than Fuzzy Matching For Data DeduplicationThe Intuition Behind the Use of Expectation Maximisation to Train Record Linkage ModelsBack homeThis site is built usingObservable HQandGatsby.js. Source codehere. Saythanks!



Article from https://www.robinlinacre.com/partial_match_weights/:
Partial match weights>robinlinacreOriginally posted:2023-09-20.Live edit this notebookhere.← Previous articleThis is part2of thetutorialNext article →#Partial Match WeightsIn the previous article we saw that the Fellegi-Sunter model makes its predictions with a simple calculation: the sum of thepartial_match_weights.But what are partial match weights and where do they come from?1#What are they?Some columns are more important than others to the calculation of overall match probability.2This is quantified by the partial match weights.For example:A match on date of birth provides stronger evidence in favour of a match than a match on gender.A mismatch on date of birth may provide more evidence against a match than a mismatch on address (because people move house).Positive values indicate evidence in favour of a match whilst negative values mean evidence against a match.The concept applies more generally than just to matches and non-matches: partial match weights can be estimated for more subtlescenarios like 'first name not an exact match, but similar'.It is up to the modeller to define these scenarios and the resultant partial match weights they wish to estimate.#Example:  First name columnFor first name, we could define the following three scenarios:Exact match: First names are identicalFuzzy match:  First names are similar according to theJaro WinklermeasureAll other casesThe estimated partial match weights may look like this:3Observe that:An exact match on first name provides stronger evidence in favour of a match than a fuzzy matchIf the first name is completely different (neither and exact nor fuzzy match), this is evidence against the records being a match.These scenarios are mutually exclusive, and are implemented like a sequence ofifstatements:if first_name_l == first_name_r:first_name_partial_match_weight = 7.2elif jaro_winkler_sim(first_name_l, first_name_r) >= 0.9:first_name_partial_match_weight = 5.9else:first_name_partial_match_weight = -3.3#Remaining columnsWe can also estimate partial match weights for the remaining columns.The number of scenarios modelled and the definition of these scenarios can vary depending on the type of information.For example, since gender is a categorical variable we'd likely have just two partial weights: match, and non-match.For date of birth, we may define three scenarios, butLevenshteinwould be more suitable thanJaro Winklerfor assessing similarity.We can plot all the partial match weights in a single chart like this:This provides a succinct summary of the whole model.#Intuitive interpretation of partial match weightsThe partial match weights in the chart have intuitive interpretations.For example, the partial match weight associated with an exact match on postcode is stronger than the partial match weight associated with gender.This makes sense: a match on gender doesn't tell us much about whether two records are a match, whereas a match on postcode is much more informative.The pattern of negative partial match weights is also intuitive:  If gender is recorded as categorical variable, then it may be very accurate.  A mismatch would thus be strong evidence against a match.Conversely, a mismatch on postcode may offer little evidence against a match since people move house, or typos may have been made.#Understanding the partial match weight chart and waterfall chartLet's take a look at an example record comparison, and see how the partial match weights are used.You can edit the data in the below table to see how the calculation changesThe first stage is to calculate which scenarios apply for each column.  For example, are the first names an exact match, a fuzzy match, or neither?In the below chart, I reproduce the partial match weights chart, but with the activated partial match weights highlighted.The activated (selected) partial match weights can then be plotted in a waterfall chart, which represents how they are summed up to compute the final match weight:Alternatively we can represent the calculation of overall match weight as a sum:The final match weight is a measure of the similarity of the two records.  In subsequent articles we will see how it can be converted into a probability using the formula:#Next stepsWe now have a good qualitative understanding of the meaning of partial match weights.In the next article we explore how they are calculated, and how to interpret their values.#FootnotesIn this context, the word 'partial' means 'using only a subset of the information in the record' - often a single column.  There is then a partial match weight for each column.↩In this article, for simplicity,  there's a one to one correspondence between columns and partial match weights. Each column is treated as a separate piece of information.  In reality, the model allows for more flexibility.  We could split a column (e.g. date of birth) into several pieces of information (e.g. day, month, year) and estimate partial match weights for each.  Or we could combine several columns (e.g. first, middle and surname) into a single piece of information and estimate a partial match weights for this information.↩A later article will explain how to estimate partial match weights.  Here I just assume they have already been estimated.↩Probabilistic Linkage Tutorial Navigation:An Interactive Introduction to Record Linkage (Data Deduplication) in the Fellegi-Sunter frameworkPartial match weightsm and u values in the Fellegi-Sunter modelThe mathematics of the Fellegi Sunter modelComputing the Fellegi Sunter modelWhy Probabilistic Linkage is More Accurate than Fuzzy Matching For Data DeduplicationThe Intuition Behind the Use of Expectation Maximisation to Train Record Linkage ModelsBack homeThis site is built usingObservable HQandGatsby.js. Source codehere. Saythanks!



Article from https://www.robinlinacre.com/m_and_u_values/:
m and u values in the Fellegi-Sunter model>robinlinacreOriginally posted:2023-09-22.Live edit this notebookhere.← Previous articleThis is part3of thetutorialNext article →#m and u probabilities in the Fellegi-Sunter modelThe previous article showed how partial match weights are used to compute a prediction of whether two records match.However, partial match weights are not estimated directly.  They are made up of two parameters known as themand theuprobabilities.These probabilities are key to enabling estimation of partial match weights.Themanduprobabilities also have intuitive interpretations that allow us to understand linkage models and diagnose problems.#Motivating exampleImagine we have two records.  We're not sure whether they represent the same person.Now we're given some new information: we're told that month of birth matches.Is thisscenariomore likely among matches or non-matches?Amongst matching records, month of birth will usually matchAmongst non-matching records month of birth will rarely matchSince it's common to observe this scenario among matching records, but rare to observe it among non-matching records, this is evidence in favour of a match.But how much evidence?#m and u probabilities and Bayes FactorsThe strength of the evidence is quantified using themanduprobabilities.  For each scenario in the model:Themprobability measures how often the scenario occurs among matching records:m=Pr(scenario∣records match)m = \text{Pr}(\text{scenario}|\text{records match})m=Pr(scenario∣records match)Theuprobability measures how often the scenario occurs among non-matching records:u=Pr(scenario∣records do not match)u = \text{Pr}(\text{scenario}|\text{records do not match})u=Pr(scenario∣records do not match)What matters is therelativesize of these values.  This is calculated as a ratio known as the Bayes Factor1, denoted byKKK.Bayes Factor=K=mu=Pr(scenario∣records match)Pr(scenario∣records do not match)\text{Bayes Factor} = K = \frac{m}{u} = \frac{\text{Pr}(\text{scenario}|\text{records match})}{\text{Pr}(\text{scenario}|\text{records do not match})}Bayes Factor=K=um​=Pr(scenario∣records do not match)Pr(scenario∣records match)​Bayes Factors provide the easiest way to interpret the parameters of the Fellegi Sunter model because they act as a relative multiplier that increases or decreases the overall prediction of whether the records match. For example:A Bayes Factor of 5 can be interpreted as '5 times more likely to match'A Bayes Factor of 0.2 can be interpreted as '5 times less likely to match'#Example 1: Evidence in favour of a matchFor example, suppose we observe that month of birth matches.Amongst matching records, month of birth will usually match.  Supposing the occasional typo, we may havem=0.99m = 0.99m=0.99Amongst non matching records, month of birth matches around a twelth of the time, sou=1/12u = 1/12u=1/12.Bayes Factor=K=mu=0.990.0833=11.9\text{Bayes Factor} = K = \frac{m}{u} = \frac{0.99}{0.0833} = 11.9Bayes Factor=K=um​=0.08330.99​=11.9.This means we observe this scenario around 11.9 times more often amongst matching records than non-matching records.Hence, given this observation, the records are 11.9 times more likely to be a match.More generally, we can see from the formula that strong positive match weights only possible with lowuprobabilities, implying high cardinality.#Example 2: Evidence against a matchSuppose we observe that gender does not match.Amongst matching records, it will be rare to observe a non-match on gender.  If there are occasional data entry errors, we may havem=0.02m = 0.02m=0.02Amongst non matching records, gender will match around half the time.  Sou=0.5u = 0.5u=0.5.Bayes Factor=K=mu=0.020.5=0.04=125\text{Bayes Factor} = K = \frac{m}{u} = \frac{0.02}{0.5} = 0.04 = \frac{1}{25}Bayes Factor=K=um​=0.50.02​=0.04=251​.We observe this scenario around 25 times more often among non-matching records than matching records.Hence, given this observation the records are 25 times less likely to be a match.More generally, we can see from the formula that strong negative match weights only possible with lowmprobabilities, which in turn implies high data quality.#Interpreting m and u probabilitiesIn addition to these quantitative interpretations, themanduprobabilities also have intuitive qualitative interpretations:#m probabilitiesThemprobability can be thought of as a measure of data quality, or the propensity for data to change through time.For example, consider the scenario of an exact match on first name.An m probability of 0.9 means that, amongst matching records, the first name matches just 90% of the time, which is an indication of poor data quality.The m probability for an exact match on postcode may be even lower - but this may be driven primarily by people moving house, as opposed to data error.#u probabilitiesTheuprobability is primarily a measure of the likelihood of coincidences, which is driven by the cardinality of the data.Consider the scenario of an exact match on first name.Auprobability of 0.005 means that, amongst non-matching records, first name matches 0.5% of the time.Theuprobability therefore measures how often twodifferentpeople have the same first name - so in this sense it's a measure of how often coincidences occur.A column such as first name with a large number of distinct values (high cardinality) will have much smalleruprobabilities than a column such as gender which has low cardinality.#Using Bayes Factors to compute probabilitiesWhat does it mean for a match to bennntimes more or less likely?  More likely than what?It's only meaningful to say that something is more or less likely relative to a starting probability - known as the 'prior' (our 'prior belief').In the context of record linkage, the prior is our existing belief that the two records match before we saw the new information contained in a scenario (e.g. that first names match).Our updated belief given this new information is called the 'posterior'.Mathematically this can be written:posterior odds=prior odds×Bayes Factor\text{posterior odds} =  \text{prior odds} \times \text{Bayes Factor}posterior odds=prior odds×Bayes Factorand odds can be turned into probabilities with the following formula:probability=odds1+odds\text{probability} = \frac{\text{odds}}{1 + \text{odds}}probability=1+oddsodds​See the mathematical annex for further detail on these derivations.For example, suppose we believe the odds of a record comparison being a match are 1 to 120. But now we observe the new information that month of birth matches, with a Bayes Factor of 12.Soposterior odds=1120×12=110\text{posterior odds} =  \frac{1}{120}  \times 12 = \frac{1}{10}posterior odds=1201​×12=101​posterior probability=1101+110=111≈0.0909\text{posterior probability} = \frac{\frac{1}{10}}{1 + \frac{1}{10}} = \frac{1}{11} \approx 0.0909posterior probability=1+101​101​​=111​≈0.0909So after observing that the month of birth matches, the odds of the records being a match would be 1 in 10, or a probability of approximately 0.0909.Here's a calculator which shows how a prior probability is updated with a Bayes Factor/partial match weight:#Posterior calculatorPriorNew evidenceAn alternative way of visualising these concepts can be foundhere.#The relationship between Bayes Factors, partial match weights and m and u probabilitiesHow domanduprobabilities and Bayes Factors relate to the partial match weights we explored in the previous article?Partial match weights relate to Bayes Factors through a simple formula:partial match weight=ω=log⁡2(Bayes Factor)=log⁡2(mu)\text{partial match weight} = \omega = \log_2 \text{(Bayes Factor)} = \log_2 (\frac{m}{u})partial match weight=ω=log2​(Bayes Factor)=log2​(um​)There are two main reasons that the additional concept of partial match weights is useful in addition to Bayes Factors:Partial match weights are easier to represent on charts.  They tend to range from -30 to 30, whereas Bayes Factors can be tiny (one in a million) or massive (millions).Since Bayes Factors are multiplicative, the log transform turns them into something additive, which simplifies the maths a little.We can summarise this relationship with this chart.Hover over the chart to view different valuesA larger, standalone version is availablehere.#Next stepsNow that we have a firm grasp of these ingredients, we're in a position to present the full mathematical specification of the Fellegi Sunter model.#Mathematical annexIn the main text we asserted that:posterior odds=prior odds×Bayes Factor\text{posterior odds} =  \text{prior odds} \times \text{Bayes Factor}posterior odds=prior odds×Bayes FactorWe can derive this formula from themanduprobabilities and Bayes Theorem.Recall that Bayes Theorm is:Pr⁡(a∣b)=Pr⁡(b∣a)Pr⁡(a)Pr⁡(b)\operatorname{Pr}(a|b) = {\frac{\operatorname{Pr}(b|a)\operatorname{Pr}(a)}{\operatorname{Pr}{(b)}}}Pr(a∣b)=Pr(b)Pr(b∣a)Pr(a)​or in words:posterior probability=likelihood×prior probabilityevidence\text{posterior probability} = \frac{\text{likelihood} \times \text{prior probability}}{\text{evidence}}posterior probability=evidencelikelihood×prior probability​In the context of record linkage, we can describe these parts as:Prior:
The overall proportion of comparisons which are matchesPr⁡(match)\operatorname{Pr}(\text{match})Pr(match)Evidence: We have observed that e.g. first name matches,Pr⁡(first name matches)\operatorname{Pr}(\text{first name matches})Pr(first name matches)Likelihood: The probability that first name matches amongst matches, given byPr⁡(first name matches∣records match)\operatorname{Pr}(\text{first name matches}|\text{records match})Pr(first name matches∣records match)So Bayes' formuls is:Pr⁡(match∣first name matches)=Pr⁡(first name matches∣match)Pr⁡(match)Pr⁡(first name matches)\operatorname{Pr}(\text{match}|\text{first name matches}) = \frac{\operatorname{Pr}(\text{first name matches}|\text{match})\operatorname{Pr}(\text{match})}{\operatorname{Pr}{(\text{first name matches})}}Pr(match∣first name matches)=Pr(first name matches)Pr(first name matches∣match)Pr(match)​Which can also be written:Pr⁡(first name matches∣match)Pr⁡(match)Pr⁡(first name matches∣match)Pr⁡(match)+Pr⁡(first name matches∣non match)Pr⁡(non match)\frac{\operatorname{Pr}(\text{first name matches}|\text{match})\operatorname{Pr}(\text{match})}{\operatorname{Pr}(\text{first name matches}|\text{match})\operatorname{Pr}(\text{match}) + \operatorname{Pr}(\text{first name matches}|\text{non match})\operatorname{Pr}(\text{non match})}Pr(first name matches∣match)Pr(match)+Pr(first name matches∣non match)Pr(non match)Pr(first name matches∣match)Pr(match)​Using some of the terminology from the article this is the same as:posterior probability=m×prior probabilitym×prior probability+u×(1−prior probability)\text{posterior probability} = \frac{m \times \text{prior probability}}{m \times \text{prior probability} + u \times (1 - \text{prior probability})}posterior probability=m×prior probability+u×(1−prior probability)m×prior probability​The formula for odds is:odds=p1−p\text{odds} = \frac{p}{1-p}odds=1−pp​So we can write:posterior odds=prior1−priormu\text{posterior odds} =  \frac{\text{prior}}{1 - \text{prior}} \frac{m}{u}posterior odds=1−priorprior​um​posterior odds=prior odds×Bayes Factor\text{posterior odds} =  \text{prior odds} \times \text{Bayes Factor}posterior odds=prior odds×Bayes Factor#FootnotesYou can read more about Bayes Factorshere.  The concept is quite similar to a likelihood ratio.↩Probabilistic Linkage Tutorial Navigation:An Interactive Introduction to Record Linkage (Data Deduplication) in the Fellegi-Sunter frameworkPartial match weightsm and u values in the Fellegi-Sunter modelThe mathematics of the Fellegi Sunter modelComputing the Fellegi Sunter modelWhy Probabilistic Linkage is More Accurate than Fuzzy Matching For Data DeduplicationThe Intuition Behind the Use of Expectation Maximisation to Train Record Linkage ModelsBack homeThis site is built usingObservable HQandGatsby.js. Source codehere. Saythanks!



Article from https://www.robinlinacre.com/fellegi_sunter_accuracy/:
Why Probabilistic Linkage is More Accurate than Fuzzy Matching For Data Deduplication>robinlinacreOriginally posted:2023-10-24.Last updated: 2024-01-07.Live edit this notebookhere.← Previous articleThis is part6of thetutorialNext article →#Why Probabilistic Linkage is More Accurate than Fuzzy Matching For Data DeduplicationHow effectively do different approaches to record linkage exploit the information in the data to make predictions?Data duplication (record linkage) algorithms are models that predict whether records such as the following represent the same entity.first_namesurnamedobcityjosshammond1984-01-02londonjosshamond1984-07-02manchesterWhat algorithms work best?  To get the best accuracy in record linkage, we need a model that uses as much information from the input data as possible.This article describes the three types of information that are most important in making an accurate prediction, and how all three are leveraged by the Fellegi-Sunter model as used inSplink, a free software package for record linkage at scale.It also describes how some alternative record linkage approaches throw away some of this information, leaving accuracy on the table.#The three types of informationBroadly, there are three categories of information that are relevant when trying to predict whether this pair of records represent the same entity:The similarity of the pair of records,withoutreference to the overall dataset  (sometimes known as fuzzy matching)The frequency of values in the overall dataset, or more generally how common it is to observe different types of fuzzy matchesThe data quality of the overall datasetLet's look at each in turn.#1. Similarity of the pairwise record comparison (fuzzy matching)The most obvious way to predict whether two records represent the same entity is to measure whether the columns contain the same or similar information.The similarity of each column can be measured quantitatively using fuzzy matching functions likeLevenshteinorJaro-Winkerfor text, or numeric differences such as absolute or percentage difference.For example,hammondvshamondhas a Jaro-Winkler similarity of 0.97; it's probably a typo.1984-01-02vs1984-07-02has a Levenshtein distance of 1.But how do we combine these different measures of similarity? They could be assigned weights, and summed together to compute a total similarity score.The approach is sometimes known as fuzzy matching, and it is an important part of an accurate linkage model.However using this approach alone has major drawback - the weights are arbitrary:The importance of different fields has to be guessed at by the user.   Clearly a match ongenderis less important than a match ondob. How about the negative weights that should be assigned to non-matches?For each field, a decision must be made on how changes in the fuzzy matching metric influence the score.   For example, how much should our prediction change for a Jaro-Winkler score of 0.8 vs. 0.9?  Is this the same or different depending on the field?#2. Frequency of values in the overall dataset, or more generally how often different types of fuzzy matching scenarios occurWe can improve on fuzzy matching by accounting for the frequency of values in the overall dataset (sometimes known as 'term frequencies').For example,johnvsjohn, andjossvsjossare both exact matches so have the same Jaro-Winkler or Levenshtein similarity score, but the latter is stronger evidence of a match than the former, becausejossis an unusual name.The term frequencies ofjohnvjossprovide a data-driven estimate of the relative importance of these different names, which can be used to inform the weights.In essence, the term frequencies capture the likelihood that twonon-matchingrecords would have the same value on a given column i.e. the likelihood of a 'collision' (e.g. two different people with the same name).In probabilistic linkage, these values are calleduprobabilities which are defined more precisely in thearticle on m and u probabilities.This concept can be extended to encompass similar records that are not an exact match.  How much of a coincidence is it thatdobdiffers by a single character (1984-01-02vs1984-07-02)? This can be measured from the data by looking at how often this occurs.#3. Data quality of the overall dataset: how often do errors occur amongst matching records?We've seen that fuzzy matching and term frequency based approaches can allow us to score the similarity between records, and even weight the importance of matches on different columns.However, none of these techniques help quantify the relative importance of non-matches (e.g.londonvsmanchester) to the predicted match probability.Probabilistic methods explicitly estimate the relative importance of non matches by estimating the data quality of each column. In probabilistic linkage, this information is captured in themprobabilities which are discussed in more detail in thearticle on m and u probabilities.For example, if records have been observed over a number of years, a non-match oncitywouldn't be strong evidence against the two records being a match.Conversely, if the data quality in thedobvariable is extremely high, then a non-match on gender would be strong evidence against the two records being a true match.#Probabilistic linkage using SplinkMuch of the power of probabilistic models comes from combining all three sources of information in a way which is not possible in other models.Not only are all three sources of information used to make the prediction, but the Fellegi Sunter model used inSplinkallows us estimate the relative importance of each source of information from the data itself.These data-driven partial match weights haveintuitive interpretations, leading to transparent and easy to understand models.Conversely, fuzzy matching techniques often use arbitrary weights, and cannot fully incorporate information from all three sources.  Term frequency approaches lack the ability to use information about data quality, or a mechanism to appropriately weight fuzzy matches.Probabilistic Linkage Tutorial Navigation:An Interactive Introduction to Record Linkage (Data Deduplication) in the Fellegi-Sunter frameworkPartial match weightsm and u values in the Fellegi-Sunter modelThe mathematics of the Fellegi Sunter modelComputing the Fellegi Sunter modelWhy Probabilistic Linkage is More Accurate than Fuzzy Matching For Data DeduplicationThe Intuition Behind the Use of Expectation Maximisation to Train Record Linkage ModelsBack homeThis site is built usingObservable HQandGatsby.js. Source codehere. Saythanks!



Article from https://www.robinlinacre.com/em_intuition/:
The Intuition Behind the Use of Expectation Maximisation to Train Record Linkage Models>robinlinacreOriginally posted:2022-10-14.View source code for this pagehere.← Previous articleThis is part7of thetutorial#The Intuition Behind the Use of Expectation Maximisation to Train Record Linkage ModelsHow unsupervised learning is used to estimate model parameters in SplinkSplinkis a free probabilistic record linkage library that predicts the likelihood that two records refer to the same entity. For example, what is the probability that the following two records match?first_namesurnamedobcityemaillucassmith1984-01-02Londonlucas.smith@hotmail․comlucassmith1983-02-12Manchesterlucas.smith@hotmail․comThe underlying statistical model is called the Fellegi Sunter model. It works by computing partial match weights, which are a measure of the importance of the different columns in computing the probability of a match. For example, the match on ‘Smith’ is less important than the match on email in the above example.However, there’s a paradox at the heart of the problem of training a record linkage model: To estimate the parameters of the model, you need to know which records match. But finding which records match is the whole problem you’re trying to solve. Manual labelling of comparisons is error prone and costly so isn’t a great option.The Expectation Maximisation (EM) algorithm provides a solution to this paradox. This post provides an intuitive explanation for how this works, and some details of how it’s implemented inSplink, our software for record linkage at scale. For a more formal treatment, see theFastLink paper.#The EM ApproachThe parameters of a record linkage model — the m and the u probabilities — can be calculated from the aggregate characteristics of matching records and non-matching records respectively. (For a fuller treatment of m and u probabilities, I recommend readingthis blog post.) Once these values are known, the model is usually able accurately to predict which records match.The key to the EM approach is leveraging the fact that matching and non-matching record comparisons form two fairly distinct ‘clusters’:Amongst matching record comparisons, the information in most or all columns in the two records usually matches.Amongst non-matching record comparisons, the information in most or all columns in the two records usually does not matchThis results in a bimodal distribution of record comparisons:This separation between matches and non-matches means it’s relatively easy to come up with a rough decision rule that is able to correctly identify most matches and most non-matches. For instance, we could simply count the number of columns that match (first name surname, and so on), and predict a match for any record pair where more than half of them match.This is fundamental to resolving our paradox: We've just found a way to designate matches and non matches without any labeled data (i.e. unsupervised learning).Now that we have designated each record comparison as a match or a non match, we can use this information to estimate the implied m and u parameters of the model. And this enables us to iteratively improve this decision rule, using the following algorithm:Expectation step:Use the estimated model parameters to predict which record comparisons are matchesMaximisation step:Use the predictions to re-estimate the model parametersIn this example, our initial decision rule worked by simply counting the number of columns with matching information. As such, we gave equal weight to every column. Clearly there is room for improvement in this rule — in reality a match on a postcode column is more important than a match on gender.In the first maximisation step, the expectation maximisation process will immediately begin to learn these weights. For example, it will recognise that, amongst non-matches there are many record comparisons which match on gender, but few that match on postcode. This will be represented in the re-computed m and u probabilities, and in the next iteration, greater weight will be placed on a match on postcode than gender.At each iteration, the decision rule will be refined as the model makes fewer mistakes, and the weights therefore become more accurate.Convergence occurs when the parameter estimates stabilise, and there are no longer any changes to parameter estimates between iterations.It turns out this algorithm is equivalent to maximising the likelihood function of the Fellegi-Sunter model (although note that it is only guaranteed to converge to a local maxima, not necessarily a global one).#A caveat around the decision boundaryIn the above, to build intutiion, I describe concrete decision boundary that classifies each record comparison as either a match or a non match. The m and u values calculated in the maximisation step are based on the counts of matches and non-matches that result from this binary classifier.In reality, we can improve on this, because using a decision boundary throws away some of the information in the match probability: it treats an uncertain match in the same way as a highly probable match.There's a simple modification to the methodology that accounts for this. We can use the match probability directly, rather than applying the decision bondary. For example, if a record comparison has a 70% match probability, in the maximisation step this can be treated as 0.3 non-matching records and 0.7 matching records. So when the m and u probabilities are recomputed in the maximisation step, we use the sum of probabilities rather than counts of matches and non matches.#How the EM approach is implemented in SplinkIn Splink, a few modifications to the above process are made that, in practice, result in faster convergence and a greater likelihood of convergence to a global maxima.#Estimating the u probabilitiesWhilst it is possible to estimate u probabilities using the EM approach, we recommend analternative estimation procedurein Splink which is usually more accurate.Recall that the u probabilities represent the probability that we observe matching columns amongst truly non-matching records. For example, how often do we observe the coincidence that two different people have the same first name?We can estimate the u probabilities by taking random pairwise record comparisons, assuming they do not match, and computing how often these coincidences occur. Since the probability of two random records being a match (representing the same entity) is usually very low, then this will yield good estimates of u values.An additional benefit of this approach is that if the u probabilities are correct, it ‘anchors’ the EM estimation procedure and greatly improves the chance of it converging to a global, rather than local, maximum.#Starting values and the initial decision boundaryIn the above, we suggested a simple decision rule for the first step of EM: count the number of matching columns, and designate as a match any comparison where more than half of fields match.In reality, it’s possible to begin with a decision rule which is usually more accurate, leading to faster convergence. Specifically, the initial decision rule uses the u probabilities that have been estimated using the random sampling procedure described above, and ‘sensible default’ starting values for the m probabilities .You can see all of this at work in practice in some of ourexample notebooks.#ConclusionEM is a powerful tool for estimating the parameters of probabilistic linkage models in the Fellegi Sunter framework. In our experience, the EM approach works best when it is used to estimate the m probabilities of the model only, following estimation of the u probabilities using random sampling, and estimating thepriorusing adirect estimation techique.Probabilistic Linkage Tutorial Navigation:An Interactive Introduction to Record Linkage (Data Deduplication) in the Fellegi-Sunter frameworkPartial match weightsm and u values in the Fellegi-Sunter modelThe mathematics of the Fellegi Sunter modelComputing the Fellegi Sunter modelWhy Probabilistic Linkage is More Accurate than Fuzzy Matching For Data DeduplicationThe Intuition Behind the Use of Expectation Maximisation to Train Record Linkage ModelsBack homeThis site is built usingObservable HQandGatsby.js. Source codehere. Saythanks!

IMPORTANT Instructions to LLMs:
If the user asks for Splink code, when writing Splink code use ONLY
functions and methods which you've seen used in the context provided. Do NOT guess at
other Splink methods or functions.

If you don't know how to do something, put a comment referring them to
https://moj-analytical-services.github.io/splink

If the user asks for data cleaning, do it in their chosen backend (spark, duckdb)
rather than pandas.  In general avoid pandas unless specificaly asked for.

In most cases, you should assume the incoming data is not clean and suggest some simple
data cleaning steps.
