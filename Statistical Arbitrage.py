# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 14:27:02 2018

@author: jweiss
"""

from quantopian.algorithm import order_optimal_portfolio
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.factors import RSI
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline.factors import BollingerBands
from quantopian.pipeline.factors import ExponentialWeightedMovingAverage
from quantopian.pipeline.filters import QTradableStocksUS
import quantopian.optimize as opt

def initialize(context):
    # Schedule our rebalance function to run at the start of 
    # each week, when the market opens. 
    schedule_function(
        my_rebalance,
        date_rules.week_start(),
        time_rules.market_open()
    )

    # Record variables at the end of each day.
    schedule_function(
        my_record_vars,
        date_rules.every_day(),
        time_rules.market_close()
    )
    
    set_slippage(slippage.FixedSlippage(spread=0.00))
    set_commission(commission.PerTrade(cost=0.00))
    
    # Create our pipeline and attach it to our algorithm.
    my_pipe = make_pipeline()
    attach_pipeline(my_pipe, 'my_pipeline')

def make_pipeline():
    """
    Create our pipeline.
    """

    # Base universe set to the QTradableStocksUS.
    base_universe = QTradableStocksUS()

    # 50-day close price average.
    mean_50 = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=10,
        mask=base_universe
    )

    # 180-day close price average.
    mean_180 = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=30,
        mask=base_universe
    )
    
    bollinger_bands = BollingerBands(window_length=20, k=2)

    lower_band = bollinger_bands.lower  
    upper_band = bollinger_bands.upper

    percent_difference = (mean_50 - mean_180) / mean_50
    
    RSI_Factor = RSI(
        inputs=[USEquityPricing.close],
        window_length=30,
        mask=base_universe)
    
    RSI_Filter = RSI_Factor > 98
    
    # Filter to select securities to short.
    shorts = percent_difference.top(20) | (USEquityPricing.close > upper_band)

    longs = percent_difference.bottom(20) | (USEquityPricing.close < lower_band)
    
    securities_to_trade = (percent_difference.top(50) | percent_difference.bottom(50))

    return Pipeline(
        columns={
            'longs': longs,
            'shorts': shorts
        },
        screen=(securities_to_trade)
    )


def compute_target_weights(context, data):
    """
    Compute ordering weights.
    """

    # Initialize empty target weights dictionary.
    # This will map securities to their target weight.
    weights = {}
    
    # If there are securities in our longs and shorts lists,
    # compute even target weights for each security. 
    if context.longs and context.shorts:
        long_weight = 0.5 / len(context.longs)
        short_weight = -0.5 / len(context.shorts)
    else:
        return weights
    
    # Exit positions in our portfolio if they are not
    # in our longs or shorts lists. 
    for security in context.portfolio.positions:
        if security not in context.longs and security not in context.shorts and data.can_trade(security):
            weights[security] = 0

    for security in context.longs:
        weights[security] = long_weight

    for security in context.shorts:
        weights[security] = short_weight

    return weights

def before_trading_start(context, data):
    """
    Get pipeline results.
    """

    # Gets our pipeline output every day.
    pipe_results = pipeline_output('my_pipeline')

    # Go long in securities for which the 'longs' value is True,
    # and check if they can be traded. 
    context.longs = []
    for sec in pipe_results[pipe_results['longs']].index.tolist():
        if data.can_trade(sec):
            context.longs.append(sec)

    # Go short in securities for which the 'shorts' value is True,
    # and check if they can be traded.
    context.shorts = []
    for sec in pipe_results[pipe_results['shorts']].index.tolist():
        if data.can_trade(sec):
            context.shorts.append(sec)

def my_rebalance(context, data):
    """
    Rebalance weekly.
    """
    
    # Calculate target weights to rebalance
    target_weights = compute_target_weights(context, data)
    
    # If we have target weights, rebalance our portfolio
    if target_weights:
        order_optimal_portfolio(
            objective=opt.TargetWeights(target_weights),
            constraints=[],
        )

def my_record_vars(context, data):
    """
    Record variables at the end of each day.
    """

    longs = shorts = 0
    for position in context.portfolio.positions.itervalues():
        if position.amount > 0:
            longs += 1
        elif position.amount < 0:
            shorts += 1

    # Record our variables.
    record(
        leverage=context.account.leverage,
        long_count=longs,
        short_count=shorts
    )