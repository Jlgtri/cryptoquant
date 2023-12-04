from dataclasses import dataclass
from datetime import datetime
from logging import Logger, getLogger
from turtle import title
from typing import Final, Iterable, List, Optional, Self, Union

from anyio import create_task_group
from anyio.to_thread import run_sync
from matplotlib.pyplot import legend, plot, show, xlabel, xticks, ylabel
from numpy import mean
from pandas import DataFrame
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

from .chart import CryptoQuantChart
from .models.public.assets.metrics.charts.asset_metric_chart import (
    AssetMetricChart,
)
from .process import deshuffle, get_charts_df
from .server import server
from .utils.filter_type import filter_type


@dataclass(init=False, frozen=True)
class CryptoQuant(object):
    port: Final[Optional[int]]

    _logger: Final[Logger]
    _charts: Final[Iterable[CryptoQuantChart]]

    def __init__(
        self: Self,
        /,
        chart: Union[CryptoQuantChart, Iterable[CryptoQuantChart]],
        port: Optional[int] = None,
        *,
        logger_name: Optional[str] = None,
    ) -> None:
        object.__setattr__(
            self, 'port', port if isinstance(port, int) else None
        )

        object.__setattr__(self, '_logger', getLogger(logger_name or __name__))
        object.__setattr__(
            self, '_charts', filter_type(chart, CryptoQuantChart)
        )

    async def run(self: Self, /) -> None:
        if not self._charts:
            return self._logger.exception('No charts to process!')
        self._logger.info('%s started!', self.__class__.__name__)
        async with create_task_group() as tg:
            if self.port:
                tg.start_soon(server, self.port)

            charts: List[AssetMetricChart] = []
            predict_charts: List[AssetMetricChart] = []
            for chart in self._charts:
                if chart.predict:
                    predict_charts += await chart.run()
                else:
                    charts += await chart.run()

            pred_fields = [
                field
                for chart in predict_charts
                for field in chart.metric.fields
            ]
            single = len(pred_fields) == 1

            df: DataFrame = await run_sync(
                get_charts_df, predict_charts + charts
            )
            self._logger.info('The shape of charts is: %sx%s', *df.shape)
            df = df.dropna(axis=0)
            price = df.iloc[:, 4 : 4 + len(pred_fields)]
            df = df.drop(df.columns[4 : 4 + len(pred_fields)], axis=1)

            p_train: DataFrame
            p_test: DataFrame
            f_train: DataFrame
            f_test: DataFrame
            p_train, p_test, f_train, f_test = train_test_split(
                price,
                df,
                test_size=min(0.5, max(0.1, 100 / len(df.index))),
                random_state=0,
            )

            rf = RandomForestRegressor(
                n_estimators=1000,
                random_state=0,
                criterion='friedman_mse',
                max_depth=None,
                min_samples_split=2,
                min_samples_leaf=1,
            )

            rf.fit(f_train.iloc[:, 1:], p_train.values.ravel())
            pred = rf.predict(f_test.iloc[:, 1:])
            errors = abs(pred - (p_test.iloc[:, 0] if single else p_test))
            self._logger.info('Mean Absolute Error: %.2f$.', mean(errors))
            mape = mean(
                100 * errors / (p_test.iloc[:, 0] if single else p_test)
            )
            self._logger.info('Accuracy: %.2f%%.', 100 - mape)

            actuals = deshuffle(
                f_test, pred_fields, (p_test,) if single else p_test
            )
            preds = deshuffle(f_test, pred_fields, (pred,) if single else pred)
            f_test.sort_values(f_test.columns.values[0], inplace=True)
            timestamps = [datetime.fromtimestamp(_) for _ in f_test.iloc[:, 0]]
            for _ in [actuals] if single else actuals:
                plot(timestamps, _, 'b-', label=_.columns[0])
            for _ in [preds] if single else pred:
                label = 'Predicted - %s' % _.columns[0]
                plot(timestamps, _, 'r-', label=label)

            xticks(rotation=60)
            legend()
            xlabel('Date')
            ylabel(predict_charts[0].metric.metric_path)
            title('Actual and Predicted Values')
            show()
        self._logger.info('%s finished!', self.__class__.__name__)
