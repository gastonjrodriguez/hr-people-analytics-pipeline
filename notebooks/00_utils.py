# Databricks notebook source
# DBTITLE 1,imports
# imports
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql.functions import col, coalesce, try_to_date, expr, lit, when, trim, array, array_repeat, lead, split, current_timestamp, last, concat_ws, collect_list, size, struct, aggregate, concat, length, regexp_replace, pmod, element_at, slice, array_sort, udf, filter, sequence, sort_array, sum, row_number
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import levenshtein, least  # v2: fuzzy_match_category()
from pyspark.sql import Column, DataFrame, Window 
from datetime import datetime
import builtins
import logging
import re




# COMMAND ----------

# DBTITLE 1,normalize_legacy_id()
# v2: legacy ID normalization
def normalize_legacy_id(column_name: str) -> Column:
    """
    Converts legacy-system IDs to standard IDs. Used in SILVER.

    The legacy system encodes IDs as 100000 + (real_id - 1). Evidence from the source CSVs:
      - 60/60 legacy PKs fit their physical position only as (legacy - 99999).
        e.g. job_postings: 100009 sits between postings 9 and 11 -> real posting 10.
      - 38/38 legacy employee_id FKs in salaries/contracts match (legacy - 99999),
        where salary_id/contract_id always equals employee_id in standard rows.
      - employees.department_id ranges 100000..100009 -> departments 1..10 (there is no department 0).
    The former "% 100000" mapped every legacy ID to the wrong entity (off by one) and made
    distinct PKs collide with existing ones (records lost in deduplication).

    Args:
        column_name (str): Name of the ID column (string from Bronze).

    Returns:
        Normalized ID as string. Non-numeric values return null (try_cast).
    """
    id_long = expr(f"try_cast({column_name} AS long)")
    return (
        when(id_long >= 100000, (id_long - 99999).cast("string"))
        .otherwise(expr(f"try_cast({column_name} AS string)"))
    )

# COMMAND ----------

# DBTITLE 1,fuzzy_match_category()
# v2: Levenshtein-based categorization with a maximum distance
def fuzzy_match_category(
    column: Column,
    categories: list,
    max_distance: int = 2,
    default_value: str = "unknown",
    null_value: str = "unknown"
) -> Column:
    """
    Maps a free-text value to the closest official category (Levenshtein distance). Used in SILVER.

    v2: replaces the per-table Levenshtein blocks. The previous logic had no threshold, so ANY
    value (e.g. 'withdrawn', 'resigned') was forced into the closest category.
    Now values farther than max_distance from every category get default_value.

    Limitation: edit distance cannot detect meaning. A value that is a real word but close to a
    category (e.g. 'inactive' is at distance 2 from 'active') is still matched to that category.
    If such values appear in the source, map them explicitly before calling this function.

    max_distance = 2: in the source data, every typo (transpositions such as 'actiev', 'ofefr',
    'permnaent', 'full_tmie') is at distance <= 2 from its category once the value is lowercased,
    trimmed and its '_vN' suffix removed (input normalization is the caller's responsibility).

    Ties are resolved by the order of `categories` (same behavior as the previous blocks).

    Args:
        column (Column): normalized input value.
        categories (list): official categories, in tie-break priority order.
        max_distance (int): maximum Levenshtein distance accepted.
        default_value (str): value when no category is close enough.
        null_value (str): value when the input is null.

    Returns:
        Column with the matched category.
    """
    distances = [levenshtein(column, lit(c)) for c in categories]
    min_distance = least(*distances) if len(distances) > 1 else distances[0]

    result = when(column.isNull(), lit(null_value)).when(min_distance > max_distance, lit(default_value))
    for category, distance in zip(categories, distances):
        result = result.when(distance == min_distance, lit(category))
    return result

# COMMAND ----------

# DBTITLE 1,strip_line_breaks()
# v2: removes CR/LF characters from string columns
def strip_line_breaks(df: DataFrame, cols: list = None) -> DataFrame:
    """
    Removes carriage returns and line feeds from string columns. Used in SILVER.

    With multiLine=true in Bronze, values such as "Ingeniería\n" are read as a single
    field, but they keep the trailing line break. trim() only removes spaces, so line
    breaks must be removed explicitly before any other transformation.

    Args:
        df (DataFrame): Bronze DataFrame.
        cols (list, optional): Columns to clean. Defaults to all string columns.

    Returns:
        DataFrame with line breaks removed from the selected columns.
    """
    if cols is None:
        cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

    return df.withColumns({
        c: regexp_replace(col(c), r"[\r\n]+", "")
        for c in cols
    })

# COMMAND ----------

# DBTITLE 1,check_potential_crammed_record()
# v2: DEPRECATED - no longer used by structural_validation().
# The trailing-nulls rule produced false positives (valid records whose last two columns
# were empty). Kept only as a record of the previous logic.
def check_potential_crammed_record(null_flags_col: str = "null_flags") -> Column:
    """
    Generates a Spark expression to detect potentially clustered records.
    It requires at least 2 trailing nulls/blanks (sum >= 2)
    null -> 1, value -> 0
    """
    return expr(f"""
        aggregate({null_flags_col}, 0, (acc, x) -> acc + x) >= 2 AND
        element_at({null_flags_col}, size({null_flags_col})) = 1 AND
        element_at({null_flags_col}, size({null_flags_col}) - 1) = 1
    """)

# COMMAND ----------

# DBTITLE 1,tokenize_crammed_csv()
def tokenize_crammed_csv(fragment: str) -> list:
    """
    Tokenizes a structurally damaged CSV fragment.

    This parser is intended for structurally crammed records where:
        - physical CSV fragments may contain structural/unmatched quotes
        - valid quoted fields may contain commas
        - commas inside valid quoted fields must be preserved
        - valid surrounding quotes are removed
        - structural/unmatched quotes are ignored
        - empty fields are returned as None

    This is not intended to replace a general-purpose RFC-compliant
    CSV parser. It is specifically designed for structurally damaged
    fragments reconstructed from physical rows.
    """

    if fragment is None:
        return []

    fields = []
    current = []

    i = 0
    in_quotes = False

    while i < len(fragment):
        char = fragment[i]

        if char == '"':
            if in_quotes:
                after_quote = (
                    fragment[i + 1]
                    if i + 1 < len(fragment)
                    else None
                )

                if after_quote is None or after_quote == ",":
                    in_quotes = False
                    i += 1
                    continue

                current.append(char)
                i += 1
                continue

            previous_char = fragment[i - 1] if i > 0 else None
            next_char = fragment[i + 1] if i + 1 < len(fragment) else None

            is_field_start = (
                previous_char is None
                or previous_char == ","
            )

            has_content_after_quote = (
                next_char is not None
                and next_char != ","
            )

            if is_field_start and has_content_after_quote:
                closing_quote = fragment.find('"', i + 1)

                if closing_quote != -1:
                    after_closing = (
                        fragment[closing_quote + 1]
                        if closing_quote + 1 < len(fragment)
                        else None
                    )

                    if after_closing is None or after_closing == ",":
                        in_quotes = True
                        i += 1
                        continue

            i += 1
            continue

        if char == "," and not in_quotes:
            fields.append("".join(current))
            current = []
        else:
            current.append(char)

        i += 1

    fields.append("".join(current))

    return [
        field.strip() if field.strip() != "" else None
        for field in fields
    ]


tokenize_crammed_csv_udf = udf(
    tokenize_crammed_csv,
    ArrayType(StringType())
)

# COMMAND ----------

# DBTITLE 1,structural_validation()
# structural validation
def structural_validation(df_raw: DataFrame, pkey_col: str, business_cols: list) -> DataFrame:
    """
    Generates a function that performs a structural validation of the records, encompassing:
    - Primary key null
    - Primary key not numeric
    - Potential crammed data cases

    v2: a row is a potential crammed parent only when the next physical row (same
    source file) has a non-numeric PK. check_potential_crammed_record() is no longer used.
    business_cols is kept in the signature for compatibility.
    """
    
    window_secuencial = Window.partitionBy("_source_file").orderBy("physical_row_idx")

    df_with_next_id = df_raw.withColumn("next_id", lead(col(pkey_col), 1).over(window_secuencial))

    return df_with_next_id.withColumn(
        "rejection_reason",
        when(col(pkey_col).isNull(), f"{pkey_col}_null")
        .when(~col(pkey_col).rlike(r"^[0-9]+$"), "potential_crammed_content")
        # v2: a record is flagged as a crammed parent ONLY when the next physical row has a
        # non-numeric PK (i.e. it carries the rest of this record). The former trailing-nulls
        # rule was removed: it quarantined valid records whose last two columns were empty.
        .when(col(pkey_col).rlike(r"^[0-9]+$") & col("next_id").isNotNull() & (~col("next_id").rlike(r"^[0-9]+$")), "potential_crammed_record")
        .otherwise(None)
        ).drop("next_id")

# COMMAND ----------

# DBTITLE 1,recover_null_pk()
# v2: surrogate keys for records whose only structural problem is a null PK
def recover_null_pk(
    df_structural_quarantine: DataFrame,
    df_ready_for_cleaning: DataFrame,
    pkey_col: str,
    business_cols: list,
    rejection_reason_col: str = "rejection_reason"
) -> tuple:
    """
    Recovers records rejected only because their primary key is null. Used in SILVER.

    Analysis of the source showed that these records are real and almost complete (only the ID
    is missing) and that none of them duplicates an existing clean record. Instead of losing
    them, a deterministic surrogate key is generated:

        SK-<first 16 chars of sha2(business columns)>

    - The 'SK-' prefix makes it explicit that the ID was generated by the pipeline (not the source).
    - Deterministic: the same record always gets the same key across runs, and identical
      records get the same key (so deduplication merges them).
    - Recovered records continue through the normal flow (casting, deduplication, business
      validation): if they have other problems (e.g. null employee_id) they are still quarantined.

    Args:
        df_structural_quarantine (DataFrame): remaining structural quarantine.
        df_ready_for_cleaning (DataFrame): records ready for cleaning.
        pkey_col (str): primary key column.
        business_cols (list): business columns (without the PK) used to build the key.

    Returns:
        (df_ready_for_cleaning + recovered records, remaining structural quarantine)
    """
    null_reason = f"{pkey_col}_null"

    df_null_pk = df_structural_quarantine.filter(col(rejection_reason_col) == null_reason)

    df_recovered = df_null_pk.withColumn(
        pkey_col,
        concat(
            lit("SK-"),
            expr(
                "substring(sha2(concat_ws('|', "
                + ", ".join(f"coalesce(cast({c} AS string), '<null>')" for c in business_cols)
                + "), 256), 1, 16)"
            )
        )
    ).drop(rejection_reason_col)

    df_ready = df_ready_for_cleaning.unionByName(df_recovered)
    df_quarantine_remaining = df_structural_quarantine.filter(col(rejection_reason_col) != null_reason)

    return df_ready, df_quarantine_remaining

# COMMAND ----------

# DBTITLE 1,resolve_to_unknown_member()
# unknown member for unresolved foreign keys (Gold)
def resolve_to_unknown_member(
    df: DataFrame,
    fk_col: str,
    dim_df: DataFrame,
    dim_key: str,
    unknown_key: str = "-1"
) -> DataFrame:
    """
    Points foreign keys that do not exist in their dimension to the dimension's "Unknown" member.
    Used in GOLD.

    Standard dimensional-modeling practice (Kimball): every foreign key of a fact must resolve to a
    row of its dimension. Records whose key is missing from the dimension (e.g. an employee that was
    quarantined in Silver) are kept, but assigned to the "Unknown" member, so totals still add up
    and BI tools do not show "(Blank)" rows.

    - Non-null values not found in the dimension -> unknown_key.
    - Null values stay null (the source did not provide a value).
    - The original value is kept in source_<fk_col> for traceability.

    Args:
        df (DataFrame): table with the foreign key.
        fk_col (str): foreign key column.
        dim_df (DataFrame): dimension table.
        dim_key (str): key column of the dimension.
        unknown_key (str): key of the dimension's Unknown member.

    Returns:
        DataFrame with fk_col resolved and the extra column source_<fk_col>.
    """
    dim_keys = dim_df.select(col(dim_key).alias("_dim_key")).distinct()

    return (
        df.withColumn(f"source_{fk_col}", col(fk_col))
        .join(dim_keys, col(fk_col) == col("_dim_key"), "left")
        .withColumn(
            fk_col,
            when(col(fk_col).isNull(), lit(None))
            .when(col("_dim_key").isNull(), lit(unknown_key))
            .otherwise(col(fk_col))
        )
        .drop("_dim_key")
    )

# COMMAND ----------

# DBTITLE 1,consolidate_quarantine()
# unify quarantine
def consolidate_quarantine(df_struct_quarantine: DataFrame, df_business_quarantine: DataFrame, problematic_cols: dict, source_table_name: str) -> DataFrame:
    """
    It unifies the quarantined records in the structural validation and the business validations.
    """
    df_struct_fixed = df_struct_quarantine.select([
        problematic_cols[c].alias(c) if c in problematic_cols else col(c)
        for c in df_struct_quarantine.columns
    ])
    return df_struct_fixed.unionByName(df_business_quarantine, allowMissingColumns=True) \
        .withColumn("source_table", lit(source_table_name)) \
        .withColumn("quarantined_at", current_timestamp())

# COMMAND ----------

# DBTITLE 1,repair_crammed_records()
def repair_crammed_records(
    df_raw: DataFrame,
    df_structural_quarantine: DataFrame,
    df_ready_for_cleaning: DataFrame,
    pkey_col: str,
    business_cols: list,
    physical_idx_col: str = "physical_row_idx",
    rejection_reason_col: str = "rejection_reason",
    record_reason: str = "potential_crammed_record",
    content_reason: str = "potential_crammed_content",
    metadata_cols: list = None,
    partition_cols: list = None
) -> tuple:
    """
    Repairs structurally crammed CSV records detected during structural validation.

    The function identifies parent/content islands, reconstructs the crammed
    content in physical order, tokenizes the reconstructed CSV fragment,
    right-aligns the parsed values against the trailing NULL suffix of the
    parent record, and returns the original ready records plus the repaired
    records.

    Repair is intentionally conservative:
        - exactly one parent record is required
        - at least one content row is required
        - parsed content cannot exceed the available NULL suffix
        - the final repaired record must contain exactly all business columns
        - ambiguous cases remain excluded from automatic repair

    Parameters
    ----------
    df_raw : DataFrame
        Original bronze DataFrame containing all physical rows.

    df_structural_quarantine : DataFrame
        Rows rejected during structural validation.

    df_ready_for_cleaning : DataFrame
        Rows that passed structural validation.

    pkey_col : str
        Primary key column. The value is preserved from the parent record and
        is never reconstructed from crammed content.

    business_cols : list
        Business columns following the primary key in their physical order.

    physical_idx_col : str
        Column preserving the original physical row order.

    rejection_reason_col : str
        Structural validation rejection reason column.

    record_reason : str
        Rejection reason identifying a potential crammed parent record.

    content_reason : str
        Rejection reason identifying a potential crammed content row.

    metadata_cols : list, optional
        Additional metadata columns to preserve. By default, metadata columns
        are inferred from df_ready_for_cleaning.

    partition_cols : list, optional
        Columns used to partition the physical ordering. When omitted,
        _source_file is used when available.

    Returns
    -------
    DataFrame
        Original structurally valid rows plus successfully repaired parent
        records.
    """

    
    "_physical_cells"


    # ------------------------------------------------------------------
    # 1. Validate input columns
    # ------------------------------------------------------------------

    required_columns = [
        pkey_col,
        physical_idx_col,
        *business_cols
    ]

    for df_name, df in [
        ("df_raw", df_raw),
        ("df_structural_quarantine", df_structural_quarantine),
        ("df_ready_for_cleaning", df_ready_for_cleaning)
    ]:
        missing_cols = [
            c for c in required_columns
            if c not in df.columns
        ]

        if missing_cols:
            raise ValueError(
                f"{df_name} is missing required columns: {missing_cols}"
            )

    if rejection_reason_col not in df_structural_quarantine.columns:
        raise ValueError(
            f"'{rejection_reason_col}' must exist in "
            "df_structural_quarantine."
        )

    # ------------------------------------------------------------------
    # 2. Infer partition columns
    # ------------------------------------------------------------------

    if partition_cols is None:

        if "_source_file" in df_structural_quarantine.columns:
            partition_cols = ["_source_file"]
        else:
            partition_cols = []

    missing_partition_cols = [
        c for c in partition_cols
        if c not in df_structural_quarantine.columns
    ]

    if missing_partition_cols:
        raise ValueError(
            f"partition_cols contain columns that do not exist: "
            f"{missing_partition_cols}"
        )

    # ------------------------------------------------------------------
    # 3. Infer metadata columns
    # ------------------------------------------------------------------

    if metadata_cols is None:

        excluded_cols = set(
            [pkey_col]
            + business_cols
            + [physical_idx_col]
            + [rejection_reason_col]
        )

        metadata_cols = [
            c
            for c in df_ready_for_cleaning.columns
            if c not in excluded_cols
        ]

    missing_metadata_cols = [
        c
        for c in metadata_cols
        if c not in df_ready_for_cleaning.columns
    ]

    if missing_metadata_cols:
        raise ValueError(
            f"metadata_cols contain columns that do not exist in "
            f"df_ready_for_cleaning: {missing_metadata_cols}"
        )

    # ------------------------------------------------------------------
    # 4. Build Gap & Islands
    # ------------------------------------------------------------------

    if partition_cols:

        island_window = (
            Window
            .partitionBy(*partition_cols)
            .orderBy(physical_idx_col)
            .rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )

    else:

        island_window = (
            Window
            .orderBy(physical_idx_col)
            .rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )

    # v2: only crammed parent/content rows take part in islands.
    # Other structural rejections (e.g. '<pk>_null') must stay in quarantine and
    # must never be absorbed by an island.
    df_quarantine = (
        df_structural_quarantine
        .filter(
            col(rejection_reason_col).isin(record_reason, content_reason)
        )
    )

    df_islands = (
        df_quarantine
        .withColumn(
            "_island_start",
            when(
                col(rejection_reason_col) == record_reason,
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn(
            "_island_id",
            spark_sum("_island_start").over(island_window)
        )
    )

    # ------------------------------------------------------------------
    # 5. Calculate island statistics
    # ------------------------------------------------------------------

    group_cols = [
        *partition_cols,
        "_island_id"
    ]

    df_island_stats = (
        df_islands
        .groupBy(*group_cols)
        .agg(
            spark_sum(
                when(
                    col(rejection_reason_col) == record_reason,
                    lit(1)
                ).otherwise(lit(0))
            ).alias("_parent_count"),

            spark_sum(
                when(
                    col(rejection_reason_col) == content_reason,
                    lit(1)
                ).otherwise(lit(0))
            ).alias("_content_count")
        )
    )

    # ------------------------------------------------------------------
    # 6. Keep only repairable islands
    # ------------------------------------------------------------------

    df_repairable_islands = (
        df_islands
        .join(
            df_island_stats,
            on=group_cols,
            how="inner"
        )
        .filter(
            (col("_parent_count") == 1)
            &
            (col("_content_count") > 0)
        )
    )

    # ------------------------------------------------------------------
    # 7. Extract parent records
    #
    # The PK is preserved separately.
    # business_cols are used for suffix alignment.
    # ------------------------------------------------------------------

    df_parent_records = (
        df_repairable_islands
        .filter(
            col(rejection_reason_col) == record_reason
        )
        .select(
            *group_cols,
            physical_idx_col,
            col(pkey_col),
            *business_cols
        )
        .withColumnRenamed(
            physical_idx_col,
            "_parent_physical_row_idx"
        )
    )

    # ------------------------------------------------------------------
    # 8. Build parent business-cell array
    # ------------------------------------------------------------------

    df_parent_records = (
        df_parent_records
        .withColumn(
            "_parent_cells",
            array(*[
                col(c).cast("string")
                for c in business_cols
            ])
        )
    )

    # ------------------------------------------------------------------
    # 9. Find last populated business field
    # ------------------------------------------------------------------

    df_parent_records = (
        df_parent_records
        .withColumn(
            "_parent_last_non_null_position",
            expr("""
                try_element_at(
                    filter(
                        sequence(
                            1,
                            size(_parent_cells)
                        ),
                        i ->
                            element_at(
                                _parent_cells,
                                i
                            ) IS NOT NULL
                    ),
                    -1
                )
            """)
        )
    )

    # ------------------------------------------------------------------
    # 10. Calculate available NULL suffix
    # ------------------------------------------------------------------

    df_parent_records = (
        df_parent_records
        .withColumn(
            "_available_suffix_slots",
            size(col("_parent_cells"))
            -
            coalesce(
                col("_parent_last_non_null_position"),
                lit(0)
            )
        )
    )

    # ------------------------------------------------------------------
    # 11. Extract crammed physical fragments
    # ------------------------------------------------------------------

    df_content_records = (
        df_repairable_islands
        .filter(
            col(rejection_reason_col) == content_reason
        )
        .select(
            *group_cols,
            physical_idx_col,
            pkey_col,
            *business_cols
            )
        )

    # Build one physical CSV fragment per content row.
    #
    # Trailing physical NULL cells are removed because they do not represent
    # meaningful content from that physical row.

    df_content_records = (
        df_content_records
        .withColumn(
            "_physical_cells",
            array(*[
                col(c).cast("string")
                for c in [pkey_col] + business_cols
                ])
        )
        .withColumn(
            "_physical_last_non_null_position",
            expr("""
                try_element_at(
                    filter(
                        sequence(
                            1,
                            size(_physical_cells)
                        ),
                        i ->
                            element_at(
                                _physical_cells,
                                i
                            ) IS NOT NULL
                    ),
                    -1
                )
            """)
        )
        .withColumn(
            "_physical_cells_trimmed",
            expr("""
                CASE
                    WHEN _physical_last_non_null_position IS NULL
                        THEN array()
                    ELSE slice(
                        _physical_cells,
                        1,
                        _physical_last_non_null_position
                    )
                END
            """)
        )
        .withColumn(
            "_physical_fragment",
            concat_ws(
                ",",
                col("_physical_cells_trimmed")
            )
        )
    )

    # ------------------------------------------------------------------
    # 12. Aggregate fragments in physical order
    # ------------------------------------------------------------------

    df_content_grouped = (
        df_content_records
        .groupBy(*group_cols)
        .agg(
            sort_array(
                collect_list(
                    struct(
                        col(physical_idx_col).alias("_idx"),
                        col("_physical_fragment").alias("_fragment")
                    )
                )
            ).alias("_ordered_fragments")
        )
        .withColumn(
            "_merged_content",
            expr("""
                aggregate(
                    _ordered_fragments,
                    CAST('' AS STRING),
                    (acc, x) ->
                        concat(
                            acc,
                            x._fragment
                        )
                )
            """)
        )
    )

    # ------------------------------------------------------------------
    # 13. Join parent with reconstructed content
    # ------------------------------------------------------------------

    df_repaired = (
        df_parent_records
        .join(
            df_content_grouped,
            on=group_cols,
            how="inner"
        )
    )

    # ------------------------------------------------------------------
    # 14. Normalize leading structural delimiter
    #
    # Examples:
    #     ",Ana,..."
    #     ",1,2,..."
    #
    # The leading delimiter represents the continuation of the parent row.
    # ------------------------------------------------------------------

    df_repaired = (
        df_repaired
        .withColumn(
            "_csv_content",
            expr("""
                CASE
                    WHEN left(_merged_content, 2) = '",'
                        THEN substring(
                            _merged_content,
                            3,
                            length(_merged_content)
                        )

                    WHEN left(_merged_content, 1) = ','
                        THEN substring(
                            _merged_content,
                            2,
                            length(_merged_content)
                        )

                    ELSE _merged_content
                END
            """)
        )
    )

    # ------------------------------------------------------------------
    # 15. Tokenize reconstructed CSV
    # ------------------------------------------------------------------

    df_repaired = (
        df_repaired
        .withColumn(
            "_parsed_fields",
            tokenize_crammed_csv_udf(
                col("_csv_content")
            )
        )
        .withColumn(
            "_parsed_field_count",
            size(col("_parsed_fields"))
        )
    )

    # ------------------------------------------------------------------
    # 16. Conservative validation
    #
    # The parsed fragment cannot contain more fields than the available
    # trailing NULL suffix.
    # ------------------------------------------------------------------

    df_repaired = (
        df_repaired
        .filter(
            col("_parsed_field_count")
            <=
            col("_available_suffix_slots")
        )
    )

    # ------------------------------------------------------------------
    # 17. Right-align parsed fields
    # ------------------------------------------------------------------

    df_repaired = (
        df_repaired
        .withColumn(
            "_missing_suffix_slots",
            col("_available_suffix_slots")
            -
            col("_parsed_field_count")
        )
        .withColumn(
            "_parsed_fields_aligned",
            expr("""
                concat(
                    array_repeat(
                        CAST(NULL AS STRING),
                        _missing_suffix_slots
                    ),
                    _parsed_fields
                )
            """)
        )
    )

    # ------------------------------------------------------------------
    # 18. Reconstruct business columns
    # ------------------------------------------------------------------

    df_repaired = (
        df_repaired
        .withColumn(
            "_parent_prefix",
            expr("""
                CASE
                    WHEN _parent_last_non_null_position IS NULL
                        THEN array()
                    ELSE slice(
                        _parent_cells,
                        1,
                        _parent_last_non_null_position
                    )
                END
            """)
        )
        .withColumn(
            "_repaired_cells",
            concat(
                col("_parent_prefix"),
                col("_parsed_fields_aligned")
            )
        )
    )

    # ------------------------------------------------------------------
    # 19. Validate final business-cell count
    # ------------------------------------------------------------------

    df_repaired = (
        df_repaired
        .filter(
            size("_repaired_cells")
            ==
            len(business_cols)
        )
    )

    # ------------------------------------------------------------------
    # 20. Remove only physical CR/LF artifacts
    #
    # No business normalization is performed here.
    # ------------------------------------------------------------------

    df_repaired = (
        df_repaired
        .withColumn(
            "_repaired_cells_clean",
            expr("""
                transform(
                    _repaired_cells,
                    x ->
                        CASE
                            WHEN x IS NULL
                                THEN NULL
                            ELSE regexp_replace(
                                x,
                                '[\\\\r\\\\n]+',
                                ''
                            )
                        END
                )
            """)
        )
    )

    # ------------------------------------------------------------------
    # 21. Unpack repaired business columns
    # ------------------------------------------------------------------

    df_repaired_records = (
        df_repaired
        .select(
            "_parent_physical_row_idx",
            col(pkey_col),
            "_repaired_cells_clean"
        )
        .select(
            "_parent_physical_row_idx",
            col(pkey_col),
            *[
                element_at(
                    col("_repaired_cells_clean"),
                    i
                ).alias(column_name)
                for i, column_name
                in enumerate(
                    business_cols,
                    start=1
                )
            ]
        )
    )

    # ------------------------------------------------------------------
    # 22. Restore original metadata columns
    #
    # Metadata belongs to the parent physical record, so retrieve it
    # directly from df_raw.
    # ------------------------------------------------------------------

    df_parent_metadata = (
        df_raw
        .select(
            col(physical_idx_col)
                .alias("_parent_physical_row_idx"),
            *[
                col(c)
                for c in metadata_cols
            ]
        )
    )

    df_repaired_records = (
        df_repaired_records
        .join(
            df_parent_metadata,
            on="_parent_physical_row_idx",
            how="left"
        )
    )

    # ------------------------------------------------------------------
    # 23. Cast repaired columns to the same schema as ready DataFrame
    # ------------------------------------------------------------------

    df_repaired_for_union = (
        df_repaired_records
        .select(
            col(pkey_col)
                .cast(
                    df_ready_for_cleaning
                    .schema[pkey_col]
                    .dataType
                )
                .alias(pkey_col),

            *[
                col(c)
                .cast(
                    df_ready_for_cleaning
                    .schema[c]
                    .dataType
                )
                .alias(c)
                for c in business_cols
            ],

            *[
                col(c)
                for c in metadata_cols
            ],

            col(
                "_parent_physical_row_idx"
            ).alias(physical_idx_col)
        )
    )

    # ------------------------------------------------------------------
    # 24. Build physical-row IDs absorbed by successful repairs
    #
    # Includes:
    #   - the parent physical row
    #   - all crammed content physical rows
    #
    # These rows must be removed from structural quarantine because their
    # data has been successfully incorporated into the repaired parent.
    # ------------------------------------------------------------------

    # v2: only islands whose repair actually succeeded (i.e. passed steps 16 and 19)
    # are removed from quarantine. Previously, every repairable island was removed,
    # so failed repairs disappeared from both the clean and the quarantine tables.
    df_successful_islands = (
        df_repaired
        .select(*group_cols)
        .distinct()
    )

    df_repaired_physical_rows = (
        df_repairable_islands
        .join(
            df_successful_islands,
            on=group_cols,
            how="inner"
        )
        .select(
            col(physical_idx_col)
        )
        .distinct()
    )

    # ------------------------------------------------------------------
    # 25. Final union
    # ------------------------------------------------------------------

    df_unified_ready = (
        df_ready_for_cleaning
        .unionByName(
            df_repaired_for_union
        )
    )

    return (
        df_unified_ready,
        df_repaired_physical_rows
    )

# COMMAND ----------

# DBTITLE 1,DOC - repair_crammed_records()
# DOCUMENTATION FOR REPAIR_CRAMMED_RECORDS():


# def repair_crammed_records(
#     df_raw: DataFrame,
#     df_structural_quarantine: DataFrame,
#     df_ready_for_cleaning: DataFrame,
#     business_cols: list,
#     physical_idx_col: str = "physical_row_idx",
#     rejection_reason_col: str = "rejection_reason",
#     record_reason: str = "potential_crammed_record",
#     content_reason: str = "potential_crammed_content",
#     metadata_cols: list = None,
#     partition_cols: list = None
# ) -> DataFrame:
#     """
#     Repairs structurally crammed records detected during structural validation
#     and reintegrates successfully repaired records into the DataFrame ready
#     for formal data cleaning.

#     ------------------------------------------------------------------------
#     PURPOSE
#     ------------------------------------------------------------------------

#     This function addresses a specific type of structural corruption that can
#     occur when reading malformed CSV data.

#     A logical record may be physically split across multiple rows. The first
#     physical row contains the beginning of the record and is identified as a
#     `potential_crammed_record`, while one or more subsequent physical rows
#     contain the remaining fields and are identified as
#     `potential_crammed_content`.

#     Example:

#         Parent row:
#             1001,John,Doe,john@example.com,,,1990-01-01

#         Content row:
#             male,10,5,2,2020-01-10,full_time,Remote,active

#     The function reconstructs these physical fragments into the original
#     logical record before the formal data-cleaning stage.

#     This function is intentionally table-agnostic. It does not contain
#     employee-specific business rules and operates only on:

#         - the ordered list of business columns
#         - the physical row ordering column
#         - structural rejection reasons
#         - optional metadata columns
#         - optional partition columns

#     ------------------------------------------------------------------------
#     PROCESS
#     ------------------------------------------------------------------------

#     The repair process consists of the following stages:

#     1. Identify crammed-record islands
#     ----------------------------------
#     Uses a Gap & Islands pattern over the physical row order.

#     A `potential_crammed_record` starts an island and subsequent
#     `potential_crammed_content` rows belong to that island until another
#     parent record starts a new island.

#     If `_source_file` exists and no partition is explicitly supplied,
#     islands are detected independently per source file.

#     This prevents physical rows from different files or independent
#     streams from being associated with each other.

#     2. Validate the island structure
#     --------------------------------
#     An island is considered repairable only when:

#         - it contains exactly one parent record
#         - it contains at least one content row

#     This is intentional defensive behavior.

#     Islands without content are not repaired because there is no evidence
#     that additional fields are available to reconstruct the record.

#     Islands with multiple parents are also not automatically repaired because
#     the association between parent and content would be ambiguous.

#     Records that do not satisfy these conditions remain in structural
#     quarantine.

#     3. Reconstruct physical content fragments
#     -----------------------------------------
#     For every content row:

#         - business columns are converted to strings
#         - trailing physical CR/LF characters are removed
#         - trailing physical NULL cells are ignored
#         - the remaining cells are reconstructed into a CSV fragment

#     Content fragments are then ordered using `physical_row_idx` and
#     concatenated without inserting an additional separator.

#     The absence of an additional separator is important because the original
#     commas are already preserved inside the physical fragments.

#     4. Tokenize the reconstructed CSV content
#     -----------------------------------------
#     The reconstructed content is parsed using a custom tokenizer.

#     The tokenizer is designed specifically for structurally damaged CSV
#     fragments and supports:

#         - empty CSV fields
#         - commas inside valid quoted fields
#         - removal of valid surrounding quotes
#         - unmatched/structural quotes caused by the corruption
#         - preservation of field order

#     It is deliberately not intended to be a complete RFC-compliant CSV
#     parser.

#     The reason for using a custom tokenizer is that a simple `split(',')`
#     would incorrectly split legitimate values such as:

#         "Feb 06, 2017"

#     into two separate fields.

#     5. Associate recovered fields with the parent record
#     -----------------------------------------------------
#     The parent record is converted into an ordered array representing the
#     original business columns.

#     The position of the last non-null parent field is identified.

#     The recovered content is then associated with the parent's trailing
#     NULL suffix.

#     For example:

#         Parent:
#             [A, B, C, NULL, NULL, NULL]

#         Recovered content:
#             [D, E]

#         Result:
#             [A, B, C, NULL, D, E]

#     When fewer fields are recovered than available suffix positions, the
#     recovered fields are left-padded with NULLs so that their right alignment
#     is preserved.

#     This is important because a NULL value before the corrupted portion is
#     not necessarily missing data caused by the corruption. It may be a valid
#     NULL belonging to the original record.

#     6. Apply defensive field-count validation
#     ------------------------------------------
#     A record is repaired only when:

#         - the parent has a valid trailing NULL suffix
#         - the recovered field count does not exceed the available suffix
#         - the final reconstructed record contains exactly the expected
#           number of business columns

#     These checks prevent the function from silently generating malformed
#     records.

#     If a record cannot satisfy these conditions, it is left outside the
#     ready-for-cleaning DataFrame and therefore remains quarantined.

#     7. Remove physical corruption artifacts
#     ----------------------------------------
#     After reconstruction, only physical CR/LF characters introduced by the
#     corrupted physical rows are removed.

#     The function intentionally does not perform business-level
#     normalization such as:

#         - correcting spelling
#         - standardizing dates
#         - changing capitalization
#         - trimming business values as a cleaning rule
#         - validating domain values
#         - correcting invalid codes

#     Those operations belong to the formal cleaning/standardization stage.

#     8. Restore the original schema
#     ------------------------------
#     Reconstructed values are cast back to the data types defined by
#     `df_ready_for_cleaning`.

#     Metadata is recovered from the original raw DataFrame.

#     The parent's original `physical_row_idx` is retained so that the
#     repaired record continues to represent the same physical source row.

#     9. Reintegrate repaired records
#     -------------------------------
#     Successfully repaired records are unioned with the DataFrame that
#     already passed structural validation.

#     The resulting DataFrame therefore contains:

#         - originally valid records
#         - successfully repaired crammed records

#     Unrepairable records remain in structural quarantine.

#     ------------------------------------------------------------------------
#     CASES CONTEMPLATED
#     ------------------------------------------------------------------------

#     The function explicitly supports:

#         - one parent row followed by one content row
#         - one parent row followed by multiple content rows
#         - records containing valid NULL values before the corrupted section
#         - empty CSV fields
#         - trailing NULL physical cells
#         - CR/LF artifacts introduced by physical row splitting
#         - quoted fields containing commas
#         - structurally unmatched quotes
#         - independent physical streams when partitioning is configured
#         - tables with different business-column counts and names

#     The function is intentionally conservative when the structure is
#     ambiguous.

#     It does NOT attempt to automatically repair:

#         - islands with no content rows
#         - islands containing multiple parent records
#         - reconstructed content containing more fields than the available
#           parent suffix
#         - records whose final field count does not match the expected schema
#         - arbitrary malformed CSV structures that cannot be safely associated
#           with a parent record

#     Such records remain quarantined for inspection or a separate repair
#     strategy.

#     ------------------------------------------------------------------------
#     WHY THIS DESIGN
#     ------------------------------------------------------------------------

#     The repair is performed before formal data cleaning because the problem
#     is structural rather than semantic.

#     At this stage the objective is to recover the original logical record
#     without changing its business meaning.

#     Keeping structural repair separate from business cleaning provides two
#     important benefits:

#         1. Structural corruption can be audited independently.
#         2. Business cleaning rules are not responsible for reconstructing
#            physically damaged records.

#     The function also avoids "guessing" values. When the structure cannot be
#     reconstructed with sufficient confidence, the record remains quarantined.

#     ------------------------------------------------------------------------
#     SCALE AND PERFORMANCE
#     ------------------------------------------------------------------------

#     This implementation is intended for situations where the structurally
#     corrupted subset is relatively small compared with the complete dataset.

#     For example, it is appropriate when processing:

#         - thousands of records
#         - tens of thousands of records
#         - hundreds of thousands of records
#         - potentially larger datasets where only a small percentage of rows
#           reach the repair stage

#     The important consideration is not only the total size of the source
#     table, but how many records actually reach this repair path.

#     The function performs several Spark-native operations such as:

#         - Window functions
#         - aggregation
#         - grouping
#         - array operations
#         - joins
#         - schema-aware casting

#     However, CSV tokenization is implemented through a Python UDF because
#     the parsing logic is custom and needs to handle both valid quoted
#     fields and structurally unmatched quotes.

#     This introduces Python serialization/execution overhead and prevents
#     that specific parsing step from being fully optimized by Spark's
#     Catalyst engine.

#     Therefore, this implementation is a practical compromise for a
#     relatively small quarantine/repair population.

#     If the same logic had to process millions of records, or if a large
#     proportion of the dataset were structurally corrupted, a more
#     Spark-native implementation would be preferable.

#     Possible alternatives at larger scale include:

#         - implementing the tokenizer entirely with Spark SQL expressions
#         - using Spark-native higher-order functions
#         - redesigning the ingestion step to preserve raw physical records
#           differently
#         - parsing the source with a CSV reader/configuration capable of
#           handling the corruption before the DataFrame is created
#         - using a dedicated distributed parsing strategy

#     The appropriate alternative would depend on the exact corruption
#     pattern and source format.

#     ------------------------------------------------------------------------
#     TRADE-OFFS
#     ------------------------------------------------------------------------

#     1. Custom tokenizer vs. standard CSV parser
#     --------------------------------------------
#     Trade-off:
#         A custom tokenizer handles the specific corruption pattern more
#         flexibly, but it is more complex and is not a complete CSV parser.

#     Benefit:
#         It can distinguish between valid quoted commas and structural
#         unmatched quotes in damaged records.

#     Cost:
#         Additional code must be maintained and edge cases should be tested
#         as new source formats are introduced.

#     2. Python UDF vs. Spark-native parsing
#     --------------------------------------
#     Trade-off:
#         The Python UDF is easier to express for this irregular parsing
#         problem, but it introduces execution overhead.

#     Benefit:
#         The parser is readable, testable, and isolated from the rest of the
#         Spark transformation logic.

#     Cost:
#         It is less performant than a fully Spark-native implementation for
#         very large repair populations.

#     3. Conservative repair vs. maximum recovery
#     --------------------------------------------
#     Trade-off:
#         The function deliberately refuses to repair ambiguous structures.

#     Benefit:
#         It reduces the risk of silently producing incorrect business
#         records.

#     Cost:
#         Some potentially recoverable records may remain quarantined and
#         require manual inspection or a specialized repair rule.

#     4. Structural repair vs. business cleaning
#     -------------------------------------------
#     Trade-off:
#         The function does not try to "fix" business values while repairing
#         the record.

#     Benefit:
#         Responsibilities remain separated and the original information is
#         preserved as much as possible.

#     Cost:
#         A second cleaning stage is still required after structural repair.

#     5. Generic function vs. table-specific logic
#     --------------------------------------------
#     Trade-off:
#         The function relies on supplied column lists, structural rejection
#         reasons and metadata rather than hard-coded employee-specific logic.

#     Benefit:
#         The same function can be reused across different tables.

#     Cost:
#         The calling pipeline must provide the correct business-column order
#         and structural-validation outputs.

#     ------------------------------------------------------------------------
#     IMPORTANT ASSUMPTION
#     ------------------------------------------------------------------------

#     The repair strategy assumes that the crammed content belongs to the
#     trailing NULL portion of the parent record.

#     In other words, the parent row provides a valid prefix of the logical
#     record and the corrupted physical rows contain the remaining fields.

#     This assumption is what makes right-alignment possible without guessing
#     where valid NULL values belong.

#     If corruption can occur in the middle of an otherwise populated record,
#     a different reconstruction strategy would be required.


#     ------------------------------------------------------------------------
#     OPERATIONAL SCALE
#     ------------------------------------------------------------------------

#     There is no fixed maximum number of source records for this function.

#     Its suitability depends primarily on the number of records that actually
#     require structural repair, rather than only on the total size of the
#     source dataset.

#     For example:

#         - A multi-million-row dataset with a few hundred corrupted records
#           can still be a reasonable use case for this implementation.

#         - A smaller dataset where a large percentage of records require
#           repair may be a poor fit because the custom Python tokenizer would
#           become a significant portion of the workload.

#     Therefore, the key operational consideration is the size of the
#     quarantine/repair population.

#     If the repair population becomes large enough for the Python UDF to be a
#     measurable bottleneck, the parsing stage should be reconsidered and
#     potentially replaced with a Spark-native implementation.

#     ------------------------------------------------------------------------
#     CONSERVATIVE REPAIR PRINCIPLE
#     ------------------------------------------------------------------------

#     The function intentionally prioritizes correctness over maximum recovery.

#     When the available structural evidence is insufficient to reconstruct a
#     record unambiguously, the function does not guess where fields should be
#     placed.

#     Instead, the record remains in structural quarantine.

#     This is a deliberate data-quality decision:

#         incorrect reconstruction
#             >
#         unrepaired record requiring investigation

#     from the perspective of downstream data integrity.

#     A successfully repaired record should therefore be considered a record
#     whose structure satisfies the validation rules defined by this function,
#     rather than simply a record for which some values could be extracted.

#     ------------------------------------------------------------------------
#     STRUCTURAL ASSUMPTION AND LIMITATION
#     ------------------------------------------------------------------------

#     The reconstruction strategy assumes that the parent row contains a
#     valid prefix of the logical record and that the crammed content belongs
#     to the trailing NULL portion of that parent.

#     This is what allows the function to use right-alignment without guessing
#     the position of valid NULL values.

#     Example:

#         Parent:
#             [A, B, C, NULL, NULL, NULL]

#         Content:
#             [D, E]

#         Repaired:
#             [A, B, C, NULL, D, E]

#     The function is therefore not intended to repair corruption where
#     missing/crammed fields occur in the middle of an otherwise populated
#     record.

#     For example:

#         [A, B, NULL, D, E, F]

#     cannot safely be reconstructed using the same right-alignment strategy
#     if the corruption may have occurred between B and D.

#     Such cases should remain quarantined or be handled by a different
#     reconstruction strategy specifically designed for that corruption
#     pattern.

#     ------------------------------------------------------------------------
#     DESIGN PRINCIPLE
#     ------------------------------------------------------------------------

#     The function follows a "repair only when structurally justified"
#     principle.

#     It does not attempt to maximize the number of recovered rows at the
#     expense of structural certainty.

#     This makes the function suitable as a controlled recovery step between:

#         raw ingestion
#             ↓
#         structural validation
#             ↓
#         structural repair
#             ↓
#         formal data cleaning
#             ↓
#         trusted dataset

#     Rather than treating structural repair as a general-purpose data-cleaning
#     operation.



#     ------------------------------------------------------------------------
#     OUTPUT
#     ------------------------------------------------------------------------

#     Returns a DataFrame containing:

#         - all records that passed structural validation
#         - all records successfully repaired by this function

#     The output preserves:

#         - the original business schema
#         - metadata columns
#         - the parent's physical row identifier

#     Structurally rejected records that cannot be safely repaired remain
#     outside the returned DataFrame and should continue through the
#     quarantine/exception-handling flow.
#     """
# ```


# COMMAND ----------

# DBTITLE 1,deduplicate_by_completeness()
# deduplicate_by_completeness()

def deduplicate_by_completeness(
    df: DataFrame,
    key_cols: list,
    business_cols: list,
    ingested_at_col: str = "_ingested_at",
    physical_idx_col: str = "physical_row_idx"
) -> DataFrame:
    """
    Deduplicates records using a hierarchical selection strategy.

    Survivor priority:
        1. Higher completeness score.
        2. More recent ingestion timestamp.
        3. Higher physical row index as deterministic tie-breaker.

    The completeness score represents the number of business columns
    containing a non-null and non-empty value.

    The score is calculated before business transformations and
    default values are applied.

    This function does not modify business data.
    """

    completeness_flags = array(*[
        when(
            col(c).isNotNull() &
            (trim(col(c).cast("string")) != ""),
            lit(1)
        ).otherwise(lit(0))
        for c in business_cols
    ])

    df_with_completeness = (
        df
        .withColumn("_completeness_flags", completeness_flags)
        .withColumn(
            "_completeness_score",
            expr("""
                aggregate(
                    _completeness_flags,
                    0,
                    (acc, x) -> acc + x
                )
            """)
        )
        .drop("_completeness_flags")
    )

    window_duplicates = (
        Window
        .partitionBy(*key_cols)
        .orderBy(
            col("_completeness_score").desc(),
            col(ingested_at_col).desc(),
            col(physical_idx_col).desc()
        )
    )

    return (
        df_with_completeness
        .withColumn(
            "_duplicate_rn",
            row_number().over(window_duplicates)
        )
        .filter(col("_duplicate_rn") == 1)
        .drop("_duplicate_rn", "_completeness_score")
    )

# COMMAND ----------

# DBTITLE 1,PIPELINE_LOG_SCHEMA -- log_pipeline_event()
PIPELINE_LOG_SCHEMA = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("entity_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,log_pipeline_event()
# function to log events
def log_pipeline_event(
    layer: str,
    entity_name: str,
    status: str,
    error_message: str = None
    ) -> None:
    """
    It records events and errors in both the Databricks driver logs
    and the unified table 'hr_database.monitoring.pipeline_logs'.
    Used in BRONZE.
    """

    allowed_statuses = {"SUCCESS", "WARNING", "ERROR"}
    status = status.upper()

    if status not in allowed_statuses:
        raise ValueError(
            f"Invalid status '{status}'. "
            f"Allowed values are: {sorted(allowed_statuses)}"
        )

    # console
    if status == "ERROR":
        logger.error(
            f"[{layer.upper()}] Error processing "
            f"'{entity_name}': {error_message}"
        )
    elif status == "WARNING":
        logger.warning(
            f"[{layer.upper()}] Warning for Entity "
            f"'{entity_name}'. {error_message}"
        )
    else:
        logger.info(
            f"[{layer.upper()}] Entity "
            f"'{entity_name}' was processed successfully."
        )

    # defensive persistence (with this, a log failure does not break the pipeline)
    try:

        log_data = [(
            "hr_analytics_pipeline",
            layer.lower(),
            entity_name.lower(),
            status,
            str(error_message) if error_message else None
        )]

        df_log = (
            spark.createDataFrame(
                log_data,
                schema=PIPELINE_LOG_SCHEMA
            )
            .withColumn(
                "logged_at",
                current_timestamp()
            )
        )

        # persist to Delta
        df_log.write.format("delta").mode("append").saveAsTable(
            "hr_database.monitoring.pipeline_logs"
        )

    except Exception as e:
        logger.warning(
            f"The record could not be written to the monitoring table "
            f"(hr_database.monitoring.pipeline_logs): {str(e)}"
        )

# COMMAND ----------

# DBTITLE 1,pct_quarantined_data()
# data quality %
def pct_quarantined_data(df_quarantine: DataFrame, df_clean: DataFrame) -> float:
    """
    Calculates the percentage of quarantined data. Used in SILVER.

    Args:
        df_quarantine (DataFrame): Quarantined DataFrame.
        df_clean (DataFrame): Clean DataFrame.

    Returns:
        float: Percentage of quarantined data over total of records.
    """
    count_quarantined = df_quarantine.count()
    count_valid = df_clean.count()
    total_quarantined_plus_valid = count_quarantined + count_valid

    if total_quarantined_plus_valid > 0:
        return builtins.round((count_quarantined / total_quarantined_plus_valid) * 100.00, 2)
    return 0.00

# COMMAND ----------

# DBTITLE 1,normalize_to_date_format()
# normalize to date format

def normalize_to_date_format(column_to_convert: str) -> Column:
    """
    Normalizes a date column to the DateType format. Used in SILVER.

    Args:
        date_column (str): Name of the date column.

    Returns:
        Normalized date column.
    """
    return coalesce(
            try_to_date(col(column_to_convert), "yyyy-MM-dd"),
            try_to_date(col(column_to_convert), "yyyy/MM/dd"),
            try_to_date(col(column_to_convert), "dd MMM yyyy"),
            try_to_date(col(column_to_convert), "MMM dd, yyyy"),
            try_to_date(col(column_to_convert), "MMM dd,yyyy"),
            try_to_date(col(column_to_convert), "MMM dd yyyy"),
            # v2: slash dates are MM/dd/yyyy in the source (21/21 non-ambiguous cases have the day in
            # 2nd position; 12/12 hire_date vs contract start_date pairs match only as MM/dd).
            # MM/dd/yyyy must be tried BEFORE dd/MM/yyyy, otherwise ambiguous dates such as
            # 02/05/2023 are read as 2 May instead of 5 Feb. dd/MM/yyyy stays as a fallback.
            try_to_date(col(column_to_convert), "MM/dd/yyyy"),
            try_to_date(col(column_to_convert), "dd/MM/yyyy"),
            # dash dates are dd-MM-yyyy in the source (21/21 non-ambiguous cases have the day first)
            try_to_date(col(column_to_convert), "dd-MM-yyyy"),
            expr(f"try_cast({column_to_convert} as date)")
            )
# COMMAND ----------

# MAGIC %md
# MAGIC ### INCREMENTAL LOADING (v3)
# MAGIC The source delivers one full extract per batch, in a folder per delivery:
# MAGIC `/Volumes/hr_database/bronze/raw_hr_data/batch_date=YYYY-MM-DD/rrhh_<table>.csv`.
# MAGIC Each pipeline run processes ONE batch per layer (the oldest pending one), and
# MAGIC `hr_database.monitoring.batch_control` records which batch each layer already processed.
# MAGIC All Delta-specific writes are isolated in `write_batch()` (and, in Silver, in the merge
# MAGIC functions), so the rest of the logic is plain Spark.

# COMMAND ----------

# DBTITLE 1,BATCH_CONTROL_TABLE -- batch control helpers
# v3: batch control
BATCH_CONTROL_TABLE = "hr_database.monitoring.batch_control"
BATCH_FOLDER_PREFIX = "batch_date="

BATCH_CONTROL_SCHEMA = StructType([
    StructField("layer", StringType(), False),
    StructField("batch_date", StringType(), False),
    StructField("status", StringType(), False)
])


def table_exists(table_name: str) -> bool:
    """True if the table can be read. Works the same in Databricks and in the local test runner."""
    try:
        spark.table(table_name).limit(0).collect()
        return True
    except Exception:
        return False


def list_batches(volume_path: str) -> list:
    """
    Batch dates ('YYYY-MM-DD') available in the Volume, oldest first. Only folders named
    'batch_date=YYYY-MM-DD' are considered (files in the Volume root are ignored).
    """
    batches = []
    for entry in dbutils.fs.ls(volume_path):
        name = entry.name.rstrip("/")
        if name.startswith(BATCH_FOLDER_PREFIX):
            batch_date = name[len(BATCH_FOLDER_PREFIX):]
            datetime.strptime(batch_date, "%Y-%m-%d")  # fails on malformed folder names
            batches.append(batch_date)
    return sorted(batches)


def processed_batches(layer: str) -> list:
    """Batch dates already processed successfully by a layer, oldest first."""
    if not table_exists(BATCH_CONTROL_TABLE):
        return []
    rows = (
        spark.table(BATCH_CONTROL_TABLE)
        .filter((col("layer") == layer) & (col("status") == "SUCCESS"))
        .select("batch_date").distinct().collect()
    )
    return sorted(r["batch_date"] for r in rows)


def next_batch(available: list, done: list) -> tuple:
    """
    Picks the batch a layer must process: the oldest available batch not processed yet.
    If every batch was already processed, the latest one is processed again: all writes are
    idempotent, so re-processing a batch produces the same tables.

    Returns:
        (batch_date, is_reprocess, pending_after): the batch to process, whether it was already
        processed, and how many batches will still be pending after this run.
    """
    if not available:
        raise RuntimeError("No batches available to process.")
    pending = [b for b in available if b not in done]
    if pending:
        return pending[0], False, len(pending) - 1
    return available[-1], True, 0


def mark_batch_processed(layer: str, batch_date: str) -> None:
    """Records that a layer processed a batch successfully (append-only audit trail)."""
    (
        spark.createDataFrame([(layer, batch_date, "SUCCESS")], schema=BATCH_CONTROL_SCHEMA)
        .withColumn("processed_at", current_timestamp())
        .write.format("delta").mode("append").saveAsTable(BATCH_CONTROL_TABLE)
    )

# COMMAND ----------

# DBTITLE 1,write_batch()
# v3: idempotent append of one batch
def write_batch(df: DataFrame, table_name: str, batch_date: str) -> None:
    """
    Writes one batch into an append-only table partitioned by '_batch_date'. Used in BRONZE.

    Idempotent: if the batch was already loaded (e.g. a re-run after a failure), only that
    batch's rows are replaced (Delta 'replaceWhere'); the other batches are not touched.
    A table created by v2 (no '_batch_date' column) is rewritten once with the new schema.
    """
    if table_exists(table_name) and "_batch_date" in spark.table(table_name).columns:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_batch_date = DATE'{batch_date}'")
            .saveAsTable(table_name)
        )
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("_batch_date")
            .saveAsTable(table_name)
        )


def read_batch(table_name: str, batch_date: str) -> DataFrame:
    """Rows of one batch of an append-only table, without the '_batch_date' column. Used in SILVER."""
    return (
        spark.table(table_name)
        .filter(col("_batch_date") == lit(batch_date).cast("date"))
        .drop("_batch_date")
    )

# COMMAND ----------

# DBTITLE 1,Silver load strategies -- add_row_hash(), merge / SCD2 state functions
# v3: SILVER LOAD STRATEGIES
#   overwrite : the table is rewritten with the batch (small catalogs and derived snapshots)
#   merge     : upsert by primary key (event tables); rows missing from the batch are flagged
#               with _deleted_in_source = true, never deleted
#   scd2      : Slowly Changing Dimension Type 2 (state tables). silver_<table>_history keeps one
#               row per version (_valid_from / _valid_to / _is_current); silver_<table> keeps the
#               current version only, with the same columns as before (Gold does not change).
#
# Changes are detected with a hash of the business columns (columns not starting with '_').
# Each strategy has two implementations with the same semantics:
#   - *_next_state(): plain Spark, returns the whole new table. Used by the local test runner
#     and by the unit tests (Delta is not available there).
#   - apply_merge() / apply_scd2(): Delta MERGE, used on Databricks. Only the changed rows are
#     written, which is what makes the load scale.
from pyspark.sql.functions import sha2

SCD2_COLUMNS = ["_row_hash", "_valid_from", "_valid_to", "_is_current", "_deleted_in_source"]
MERGE_COLUMNS = ["_row_hash", "_deleted_in_source", "_last_changed_batch"]


def business_columns(df: DataFrame) -> list:
    return [c for c in df.columns if not c.startswith("_")]


def add_row_hash(df: DataFrame) -> DataFrame:
    """Adds _row_hash: SHA-256 of the business columns (nulls are represented explicitly)."""
    values = [coalesce(col(c).cast("string"), lit("<null>")) for c in business_columns(df)]
    return df.withColumn("_row_hash", sha2(concat_ws("||", *values), 256))


def assert_unique_key(df: DataFrame, key: str, table_name: str) -> None:
    """MERGE needs one source row per key: fail loudly instead of producing a wrong merge."""
    duplicated = df.groupBy(key).count().filter(col("count") > 1).limit(5).collect()
    if duplicated or df.filter(col(key).isNull()).limit(1).count() > 0:
        raise ValueError(f"{table_name}: '{key}' must be unique and not null before merging. Duplicates: {[r[key] for r in duplicated]}")


def _batch_lit(batch_date: str) -> Column:
    return lit(batch_date).cast("date")


def scd2_new_versions(source: DataFrame, batch_date: str) -> DataFrame:
    return (
        add_row_hash(source)
        .withColumn("_valid_from", _batch_lit(batch_date))
        .withColumn("_valid_to", lit(None).cast("date"))
        .withColumn("_is_current", lit(True))
        .withColumn("_deleted_in_source", lit(False))
    )


def merge_new_rows(source: DataFrame, batch_date: str) -> DataFrame:
    return (
        add_row_hash(source)
        .withColumn("_deleted_in_source", lit(False))
        .withColumn("_last_changed_batch", _batch_lit(batch_date))
    )


def scd2_next_state(history: DataFrame, source: DataFrame, key: str, batch_date: str) -> DataFrame:
    """
    New SCD2 history after applying one full-extract batch (plain Spark).
      - key not in history          -> new version
      - same hash as current version -> unchanged
      - different hash               -> current version closed (_valid_to = batch) + new version
      - key missing from the batch   -> current version closed, _deleted_in_source = true
    Re-applying the same batch changes nothing (idempotent).
    """
    new_versions = scd2_new_versions(source, batch_date)
    if history is None:
        return new_versions

    current = history.filter(col("_is_current"))
    closed_before = history.filter(~col("_is_current"))
    source_keys = new_versions.select(key)

    unchanged = current.join(new_versions.select(key, "_row_hash"), [key, "_row_hash"], "left_semi")
    changed = current.join(unchanged.select(key), key, "left_anti").join(source_keys, key, "left_semi")
    deleted = current.join(source_keys, key, "left_anti")

    closed_now = (
        changed.withColumn("_valid_to", _batch_lit(batch_date)).withColumn("_is_current", lit(False))
        .unionByName(
            deleted.withColumn("_valid_to", _batch_lit(batch_date)).withColumn("_is_current", lit(False))
            .withColumn("_deleted_in_source", lit(True))
        )
    )
    opened_now = new_versions.join(unchanged.select(key), key, "left_anti")

    return (
        closed_before.unionByName(unchanged).unionByName(closed_now)
        .unionByName(opened_now.select(*history.columns))
    )


def merge_next_state(target: DataFrame, source: DataFrame, key: str, batch_date: str) -> DataFrame:
    """
    New table after upserting one full-extract batch by key (plain Spark).
      - new or changed rows (or rows that come back after being flagged) -> batch version
      - unchanged rows                                                    -> kept as they are
      - rows missing from the batch -> kept, _deleted_in_source = true (first time only)
    """
    new_rows = merge_new_rows(source, batch_date)
    if target is None:
        return new_rows

    unchanged = target.filter(~col("_deleted_in_source")).join(new_rows.select(key, "_row_hash"), [key, "_row_hash"], "left_semi")
    upserted = new_rows.join(unchanged.select(key), key, "left_anti")
    missing = target.join(new_rows.select(key), key, "left_anti")
    newly_deleted = (
        missing.filter(~col("_deleted_in_source"))
        .withColumn("_deleted_in_source", lit(True))
        .withColumn("_last_changed_batch", _batch_lit(batch_date))
    )
    already_deleted = missing.filter(col("_deleted_in_source"))

    return (
        unchanged.unionByName(upserted.select(*target.columns))
        .unionByName(newly_deleted).unionByName(already_deleted)
    )

# COMMAND ----------

# DBTITLE 1,apply_scd2() / apply_merge() -- Delta MERGE
# v3: Delta implementations (Databricks). Same semantics as scd2_next_state() / merge_next_state().

def _has_columns(table_name: str, columns: list) -> bool:
    return table_exists(table_name) and set(columns) <= set(spark.table(table_name).columns)


def apply_scd2(source: DataFrame, history_table: str, key: str, batch_date: str) -> None:
    new_versions = scd2_new_versions(source, batch_date)
    if not _has_columns(history_table, SCD2_COLUMNS):
        new_versions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(history_table)
        return

    from delta.tables import DeltaTable

    current = spark.table(history_table).filter(col("_is_current")).select(key, col("_row_hash").alias("_current_hash"))
    changed = (
        new_versions.join(current, key, "inner")
        .filter(col("_row_hash") != col("_current_hash")).drop("_current_hash")
    )
    # staged rows: every batch row keyed by its key (to close the current version when it changed),
    # plus the changed rows again with a null key (so they never match and are inserted as new versions)
    staged = (
        new_versions.withColumn("_merge_key", col(key))
        .unionByName(changed.withColumn("_merge_key", lit(None).cast(new_versions.schema[key].dataType)))
    )
    batch = f"DATE'{batch_date}'"
    (
        DeltaTable.forName(spark, history_table).alias("h")
        .merge(staged.alias("s"), f"h.`{key}` = s._merge_key AND h._is_current")
        .whenMatchedUpdate(condition="h._row_hash <> s._row_hash",
                           set={"_valid_to": batch, "_is_current": "false"})
        .whenNotMatchedInsert(values={c: f"s.`{c}`" for c in new_versions.columns})
        .whenNotMatchedBySourceUpdate(condition="h._is_current",
                                      set={"_valid_to": batch, "_is_current": "false", "_deleted_in_source": "true"})
        .execute()
    )


def apply_merge(source: DataFrame, table_name: str, key: str, batch_date: str) -> None:
    new_rows = merge_new_rows(source, batch_date)
    if not _has_columns(table_name, MERGE_COLUMNS):
        new_rows.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        return

    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, table_name).alias("t")
        .merge(new_rows.alias("s"), f"t.`{key}` = s.`{key}`")
        .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash OR t._deleted_in_source")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(condition="NOT t._deleted_in_source",
                                      set={"_deleted_in_source": "true", "_last_changed_batch": f"DATE'{batch_date}'"})
        .execute()
    )

# COMMAND ----------

# DBTITLE 1,write_silver() / write_quarantine() / latest_batch()
# v3: Silver writes by strategy

def write_silver(df: DataFrame, entity: str, load_config: dict, batch_date: str) -> None:
    """
    Persists one entity of the batch according to its load strategy (see SILVER_LOAD_CONFIG in
    02_silver_transformation). For 'scd2', silver_<entity> is rebuilt from the current versions
    with the same columns as the batch, so downstream code reads it as before.
    """
    table_name = f"hr_database.silver.silver_{entity}"
    strategy = load_config["strategy"]

    if strategy == "overwrite":
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        return

    key = load_config["key"]
    assert_unique_key(df, key, table_name)

    if strategy == "merge":
        apply_merge(df, table_name, key, batch_date)
    elif strategy == "scd2":
        history_table = f"{table_name}_history"
        apply_scd2(df, history_table, key, batch_date)
        current = spark.table(history_table).filter(col("_is_current")).select(*df.columns)
        current.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    else:
        raise ValueError(f"Unknown load strategy '{strategy}' for {entity}")


def write_quarantine(df: DataFrame, entity: str, batch_date: str) -> None:
    """Quarantine is kept per batch (append, idempotent): each batch's rejected rows stay auditable."""
    write_batch(df.withColumn("_batch_date", _batch_lit(batch_date)), f"hr_database.silver.silver_quarantine_{entity}", batch_date)


def latest_batch(df: DataFrame) -> DataFrame:
    """Rows of the most recent batch of a per-batch table (e.g. quarantine)."""
    last = df.agg(expr("max(_batch_date)")).collect()[0][0]
    return df.filter(col("_batch_date") == lit(last))
