# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 10:08:50 2018

@author: jweiss
"""


from __future__ import division
import numpy as np
from sklearn import preprocessing
from sklearn.svm import LinearSVC
import pandas as pd
import os
from collections import Counter


# difference above S&P500 to be considered a good performer
how_much_better = 0.15

# fundamental and financial measures used for analysis
FEATURES = ['DE Ratio',
            'Trailing P/E',
            'Price/Sales',
            'Price/Book',
            'Profit Margin',
            'Operating Margin',
            'Return on Assets',
            'Return on Equity',
            'Revenue Per Share',
            'Market Cap',
            'Enterprise Value',
            'Forward P/E',
            'PEG Ratio',
            'Enterprise Value/Revenue',
            'Enterprise Value/EBITDA',
            'Revenue',
            'Gross Profit',
            'EBITDA',
            'Net Income Avl to Common ',
            'Diluted EPS',
            'Earnings Growth',
            'Revenue Growth',
            'Total Cash',
            'Total Cash Per Share',
            'Total Debt',
            'Current Ratio',
            'Book Value Per Share',
            'Cash Flow',
            'Beta',
            'Held by Insiders',
            'Held by Institutions',
            'Shares Short (as of',
            'Short Ratio',
            'Short % of Float',
            'Shares Short (prior ']


# label an observation as 1 or 0 based on relative performance
def status_calc(stock, sp500):
    difference = stock - sp500

    if difference > how_much_better:
        return 1
    else:
        return 0


def build_data_set(sector):

    # load the stock observations for the current sector into a data frame
    data_df = pd.DataFrame.from_csv(sector + "_data_WITH_NA.csv")

    # shuffle the observations randomly to avoid bias and get more accurate future predictions
    data_df = data_df.reindex(np.random.permutation(data_df.index))

    # replace NaN and N/A with outliers which should eventually be ignored or accounted for
    data_df = data_df.replace("NaN", -99999).replace("N/A", -99999)

    # add a new column which labels observations according to the new parameters
    data_df["Status2"] = list(map(status_calc, data_df["stock_p_change"], data_df["sp500_p_change"]))

    # create a numpy array of the fundamental measures defined above
    X = np.array(data_df[FEATURES].values)

    # cleaning, normalizing, and transforming the data
    X = preprocessing.scale(X)

    # add the created labels to a list
    y = (data_df["Status2"].values.tolist())

    Z = np.array(data_df[["stock_p_change", "sp500_p_change"]])

    return X, y, Z


def analysis():

    # list for the stocks identified as probable good performers
    invest_list = []

    # obtain a list of sector files which contain their respective stocks
    file_list = os.listdir("sector_ticker_lists")

    # iterate through each file (sector)
    for each_file in file_list[1:]:

        sector = each_file.split(".csv")[0]

        print "Building data set: " + sector
        X, y, Z = build_data_set(sector)

        print "Fitting data: " + sector

        # set clf to sklearn's linear support vector classification
        clf = LinearSVC()

        # fit the SVM model according to the given training data.
        clf.fit(X, y)

        print "Making predictions: " + sector

        # load a data frame containing the data to be predicted on
        data_df = pd.DataFrame.from_csv(sector + "_forward_sample_WITH_NA.csv")
        data_df = data_df.replace("N/A", -99999).replace("NaN", -99999)

        X = np.array(data_df[FEATURES].values)

        X = preprocessing.scale(X)

        Z = data_df["Ticker"].values.tolist()

        # iterate through each observation
        for i in range(len(X)):

            # make a prediction for the current observation
            p = clf.predict(X[i])[0]

            # if the value is a 1, the stock is expected to be a good performer
            if p == 1:
                invest_list.append(Z[i])

    return invest_list


# number of times to run through the classifier training and predictions
loops = 8


# a list to contain the predicted stocks for all loops
final_list = []

for x in range(loops):
    stock_list = analysis()
    for e in stock_list:
        final_list.append(e)
    print "Loop:" + str(x + 1)


# determine how many times a unique stock appears in final_list
x = Counter(final_list)

print(15 * "_")
print "PROSPECTIVE STOCK LIST"
save = "PROSPECTIVE_STOCK_LIST_initial.csv"

number = 0

with open(save, 'w') as f:

    for each in x:

        # if the stock appears in more than 2/3 of the loops, write the ticker to a file
        if x[each] > loops - (loops / 3):
            f.write(each + '\n')
            number += 1
    f.close()


print "Number of stocks:" + str(number)
print "Analysis Complete!"
 