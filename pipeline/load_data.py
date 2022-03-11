import pandas as pd


def merge_with_jsons(sdf, spark):
    # reading for now done in pandas not spark
    # some jsons have undesirable format (dict of columns)
    # since these are metadata files this might not be a big problem
    # (number of records may grow in dblp but number of types, journals hopefully stays bounded)
    json_filenames = [
        "pbooktitlefull.json",
        "pbooktitle.json",
        "pjournalfull.json",
        "pjournal.json",
        "ptype.json",
    ]
    col_names = [
        # id, value
        ("pbooktitlefull_id", "book_title_full"),
        ("pbooktitle_id", "book_title"),
        ("pjournalfull_id", "journal_full"),
        ("pjournal_id", "journal"),
        ("ptype_id", "source_type"),
    ]
    jsons = [spark.createDataFrame(pd.read_json(f"data/{f}")) for f in json_filenames]
    for jtable, (id_col, value_col) in zip(jsons, col_names):
        jtable = jtable.withColumnRenamed("id", id_col).withColumnRenamed(
            "name", value_col
        )
        sdf = sdf.join(jtable, on=id_col, how="left_outer")
        sdf = sdf.drop(id_col)

    return sdf


def load_inputs(path, spark):
    return (
        spark.read.options(header=True).csv(path).withColumnRenamed("_c0", "input_id")
    )


def merge_inputs_with_data(inputs, data):
    def suffix_cols(sdf, suffix, except_for="pkey"):
        for col in sdf.columns:
            if col == except_for:
                continue
            sdf = sdf.withColumnRenamed(col, col + suffix)

        return sdf

    data1 = suffix_cols(data, "_x")
    data2 = suffix_cols(data, "_y")

    data1 = data1.withColumnRenamed("pkey", "key1")
    data2 = data2.withColumnRenamed("pkey", "key2")

    inputs = inputs.join(data1, on="key1", how="left")
    inputs = inputs.join(data2, on="key2", how="left")
    return inputs


def load_data(spark):
    dblp = (
        spark.read.options(header=True)
        .csv("data/dblp")
        .withColumnRenamed("_c0", "original_id")
    )
    sdf = merge_with_jsons(dblp, spark)
    return sdf
