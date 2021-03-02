from open_mastr.data_io import read_raw_data, filter, save_cleaned_data


def cleaned_data(save_csv=True):
    """
    Cleanes raw data while preserving columns and its names

    Cleaning includes:

    * Removal of duplicates originating from migrated and directly entered data in MaStR which describes the same unit

    Parameters
    ----------
    save_csv: bool
        If :obj:`True`, cleaned data will be saved to CSV files.

    Returns
    -------
    dict
        Cleaned open-MaStR unit data
    """

    # Read raw data
    raw = read_raw_data()

    # Filter data to remove duplicates originating from migrated and directly entered data
    raw_filtered = {k: filter(df) for k, df in raw.items()}

    if save_csv:
        save_cleaned_data(raw_filtered)

    return raw_filtered