from enum import auto
from enum import StrEnum

import numpy as np
from eemeter.eemeter import HourlyBaselineData
from eemeter.eemeter import HourlyModel
from eemeter.eemeter.common.features import estimate_hour_of_week_occupancy
from eemeter.eemeter.common.features import fit_temperature_bins
from eemeter.eemeter.models.hourly.design_matrices import (
    create_caltrack_hourly_preliminary_design_matrix,
)
from eemeter.eemeter.models.hourly.design_matrices import (
    create_caltrack_hourly_segmented_design_matrices,
)
from eemeter.eemeter.models.hourly.model import fit_caltrack_hourly_model
from eemeter.eemeter.models.hourly.segmentation import segment_time_series
from eemeter.eemeter.models.hourly.wrapper import IntermediateModelVariables

month_dict = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


class EEMeterSegmentType(StrEnum):
    three_month_weighted = auto()
    single = auto()


def create_and_fit_hourly_model(
    data: HourlyBaselineData,
    segment_type: EEMeterSegmentType,
    include_occupancy: bool,
) -> HourlyModel:
    model = HourlyModel()
    model.segment_type = segment_type
    meter_data = data.df["observed"].to_frame("value")
    temperature_data = data.df["temperature"]

    model.model_process_variables = IntermediateModelVariables()

    # preliminary design matrix
    preliminary_design_matrix = create_caltrack_hourly_preliminary_design_matrix(
        meter_data, temperature_data
    )
    model.model_process_variables.preliminary_design_matrix = preliminary_design_matrix

    # segment time series
    segmentation = segment_time_series(preliminary_design_matrix.index, model.segment_type)
    model.model_process_variables.segmentation = segmentation

    # estimate occupancy
    occupancy_threshold = 0.65 if include_occupancy else -1
    occupancy_lookup = estimate_hour_of_week_occupancy(
        preliminary_design_matrix, segmentation=segmentation, threshold=occupancy_threshold
    )
    model.model_process_variables.occupancy_lookup = occupancy_lookup

    # fit temperature bins
    (occupied_t_bins, unoccupied_t_bins) = fit_temperature_bins(
        preliminary_design_matrix,
        segmentation=segmentation,
        occupancy_lookup=occupancy_lookup,
    )
    model.model_process_variables.occupied_temperature_bins = occupied_t_bins
    model.model_process_variables.unoccupied_temperature_bins = unoccupied_t_bins

    # create segmented design matrices
    segmented_design_matrices = create_caltrack_hourly_segmented_design_matrices(
        preliminary_design_matrix,
        segmentation,
        occupancy_lookup,
        occupied_t_bins,
        unoccupied_t_bins,
    )
    model.model_process_variables.segmented_design_matrices = segmented_design_matrices

    # fit model
    model.model = fit_caltrack_hourly_model(
        segmented_design_matrices,
        occupancy_lookup,
        occupied_t_bins,
        unoccupied_t_bins,
        model.segment_type,
    )
    model.is_fit = True
    model.model_metrics = model.model.totals_metrics

    # calculate baseline residuals
    prediction = model.model.predict(temperature_data.index, temperature_data)
    meter_data = meter_data.merge(prediction.result, left_index=True, right_index=True)
    meter_data = meter_data.dropna()
    meter_data["resid"] = meter_data["value"] - meter_data["predicted_usage"]

    # get uncertainty variables
    model._autocorr_unc_vars = {}
    if list(model.model_metrics.keys()) == ["all"]:
        model._autocorr_unc_vars["all"] = {
            "mean_baseline_usage": np.mean(meter_data["value"]),
            "n": model.model_metrics["all"].observed_length,
            "n_prime": model.model_metrics["all"].n_prime,
            "MSE": np.mean(meter_data["resid"] ** 2),
        }
    else:
        # monthly segment model
        model_month_dict = {
            k.replace("-weighted", "").split("-")[1]: k for k in model.model_metrics
        }
        meter_data["month"] = meter_data.index.month

        for month_abbr, model_key in model_month_dict.items():
            month_n = month_dict[month_abbr]
            month_data = meter_data[meter_data["month"] == month_n]

            model._autocorr_unc_vars[month_n] = {
                "mean_baseline_usage": np.mean(month_data["value"]),
                "n": model.model_metrics[model_key].observed_length,
                "n_prime": model.model_metrics[model_key].n_prime,
                "MSE": np.mean(month_data["resid"] ** 2),
            }

    return model
