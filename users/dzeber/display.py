
"""
Utilities for formatting and displaying output in the Jupyter notebook.
"""

from __future__ import division
import IPython.display as IPDisplay
import os.path
#from pandas import DataFrame as PDF
import sys

PANDAS_CSS_FILE = "pandas_df.css"


def prettify_pandas():
    """ Modify the appearance of Pandas DataFrames when rendered as notebook
        cell output:

        - add custom CSS to improve the appearance
        - if the indexing is "trivial" (the possibly reordered default index),
          attempt to display it as row numbers starting from 1.

        The CSS is read from a file in the package dir. The re-indexing doesn't
        modify the DataFrame itself. Note that this is done by monkey-patching
        the pandas module.
    """
    ## Update the IPython display method with the custom CSS.
    try:
        css_path = os.path.join(os.path.dirname(__file__), PANDAS_CSS_FILE)
        with open(css_path) as f:
            css_str = f.read()
        IPDisplay.display(IPDisplay.HTML("<style>{}</style>".format(css_str)))
        print("Updated the display CSS.")
    except IOError:
        print("Could not set Pandas styles: CSS file not found.")

    try:
        pandas_module = sys.modules["pandas"]
        def new_repr_html(self):
            """ Attempt to reset the index to row numbers, and render
                the DataFrame as HTML.
            """
            return number_rows_if_trivial_index(self).to_html()
        pandas_module.DataFrame._repr_html_ = new_repr_html
        print("Patched the pandas module to display with row numbering.")
    except KeyError:
        print("Could not set row numbering for display: " +
              "could not find the pandas module.")


def md_print(markdown_text):
    """ Print Markdown text so that it renders correctly in the cell output. """
    IPDisplay.display(IPDisplay.Markdown(markdown_text))


def print_count(n, description=None, n_overall=None,
                                     overall_description=None,
                                     show_n_overall=True):
    """ Print a nicely formatted count of elements, optionally with a
        description, and optionally with a percentage out of a total.
    
        n: an integer count, which is nicely formatted for printing.
        description: a string prepended as "<description>: <n>".
        n_overall: a number of total elements, out of which the given count is
                   a subset. The percentage will computed and appended as
                   "<n> out of <n_overall>  (<pct>%)".
        overall_description: a string describing the overall set of elements,
                             of which the count is a subset. This is useful when
                             the overall group is itself a subset of a more
                             general population.
        show_n_overall: should the overall count itself be printed, or just
                        the percentage?
    """
    n_fmt = "{:,}".format(n)
    if n_overall:
        n_overall_fmt = "{:,}".format(n_overall)
        pct_fmt = "{:.2f}%".format(n / n_overall * 100)
        if show_n_overall:
            tot_str = " out of {tot}"
            if overall_description:
                tot_str += " {tot_descr}"
            pct_str = "({pct})"
        else:
            tot_str = ""
            pct_str = "({pct}"
            if overall_description:
                pct_str += " of {tot_descr}"
            pct_str += ")"
        overall_str = tot_str + "  " + pct_str
        overall_str = overall_str.format(tot=n_overall_fmt,
                                         pct=pct_fmt,
                                         tot_descr=overall_description)
    else:
        overall_str = ""
    final_str = "{n}" + overall_str
    if description:
        final_str = "{descr}:  " + final_str
    final_str_fmt = final_str.format(n=n_fmt,
                                     descr=description)
    print(final_str_fmt)


def show_df(DF, n_rows=10):
    """ Display the first few rows of the Spark DataFrame as a Pandas DataFrame.
    """
    return DF.limit(n_rows).toPandas()


def df_show_count_pct(df, n_overall=None, count_col="count",
                      order_by_count=False, show_cum_pct=False):
    """ Format a pandas df for displaying counts.
    
        Add percentages and format the numbers for printing.
        
        Supply the total count out of which percentages should be
        computed. If missing, the sum of counts in the table will be used.
        If a string, it will be taken as a column name containing
        corresponding group counts.
        
        Optionally order the df by decreasing count.
        
        Optionally show the cumulative percentage.
    """
    if not n_overall:
        n_overall = df[count_col].sum()
    elif isinstance(n_overall, basestring):
        n_overall = df[n_overall]
    if order_by_count:
        df = df.sort_values(count_col, ascending=False)
    ## Reset row index after sorting to start from 1.
    #df.reset_index(drop=True, inplace=True)
    #df.index += 1
    df["%"] = df[count_col] / n_overall * 100
    if show_cum_pct:
        df["cum %"] = df[count_col].cumsum() / n_overall * 100
        df["cum %"] = df["cum %"].map("{:.2f}".format)
    df[count_col] = df[count_col].map("{:,}".format)
    df["%"] = df["%"].map("{:.2f}".format)
    return df


def df_row_numbers_from_one(df):
    """ Reset the index of a DataFrame (in place) to row numbers, starting from
        1.
    """
    df.reset_index(drop=True, inplace=True)
    df.index += 1


def number_rows_if_trivial_index(df):
    """ If the current index of a DataFrame is the trivial one, consisting of
        integers 0 to len(index) - 1, copy it and reset the index to number rows
        from 1.
    """
    try:
        sorted_index = sorted(df.index)
        ## Check for the trivial index:
        ## after sorting, the integers 0 through len(index) - 1.
        is_trivial = True
        for i, ind_val in enumerate(sorted_index):
            if int(ind_val) != i:
                is_trivial = False
                break

        if is_trivial:
            df_final = df.copy()
            df_row_numbers_from_one(df_final)
        else:
            df_final = df
        return df_final
    except:
        ## If there is a problem checking the index, bail.
        return df


#def display_pd_dataframes_numbered_from_one(obj):
#    """ If the given object is a pandas DataFrame, try to renumber the rows
#        from 1.
#    """
#    if isinstance(obj, PDF):
#        return number_rows_if_trivial_index(obj)
#    else:
#        return obj
#
