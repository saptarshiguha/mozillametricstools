
"""
Utilities for formatting and displaying output in the Jupyter notebook.
"""

from __future__ import division
import IPython.display as IPDisplay
import os.path
from pandas import DataFrame as PDF
from pandas.formats.style import Styler as PDStyler
from numpy import isnan as npisnan
import sys
from matplotlib import colors as pltcolors
from matplotlib.cm import get_cmap as pltcmap

PANDAS_CSS_FILE = "pandas_df.css"


def md_print(markdown_text):
    """ Print Markdown text so that it renders correctly in the cell output. """
    IPDisplay.display(IPDisplay.Markdown(markdown_text))


def time_msg(message):
    """ Print a message together with the current date and time.

        This is printed in bold using Markdown display, so that it stands out
        in the notebook output.
    """
    msg_str = "{msg}: {time}".format(msg=message, time=now())
    msg_str_fmt = "__{}__".format(msg_str)
    md_print(msg_str_fmt)


def validate_condition(descr, code):
    """ An assertion check wrapped with some formatting.

        Failures don't halt the flow. This is just meant as a visual description
        as a part of the notebook. The provided code is evaluated and the result
        is printed next to a description.

        descr: a string describing what the assertion is meant to check
        code: code to verfiy the assertion. It should generally evaluate to a
              boolean, although it doesn't have to.
    """
    md_print("__Assert__ {descr}:&nbsp;&nbsp; `{result}`".format(
        descr=descr, result=code))


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


def now():
    """ Human-readable string giving current date and time. """
    return datetime.datetime.now().ctime()


def today():
    """ ISO-formatted date string. """
    return datetime.date.today().isoformat()


#-----------------------------------------------------------------------------
#
# Pandas display functions.


def replace_null_values(pdf, replacement):
    """ Replace null values (None or NaN) in a Pandas DataFrame.

        replacement: a single value to replace all null values with, eg. 0, or
                     a dict mapping column names to the null replacement value
                     to use in that column.

        Modifies the original df in place, doesn't return anything.
    """
    def repl_null(val, repl):
        try:
            if npisnan(val):
                return repl
        except TypeError:
            pass

        if val is None:
            return repl
        return val

    if not isinstance(replacement, dict):
        cols = list(pdf.columns)
        replacement = {coln: replacement for coln in cols}
    for coln, repl in replacement.iteritems():
        pdf[coln] = pdf[coln].apply(lambda v: repl_null(v, repl))


def fmt_pdf(pdf, format_int=True, format_float=True, float_num_dec=2):
    """ Apply number formatting to a Pandas DataFrame in preparation
        for rendering.

        Note that null values in numeric columns cause the column to get treated
        as a float type.

        pdf: a Pandas DataFrame or Styler object.
        format_int: should integer formatting (comma-separation) be applied? If
                    a single column name string or list of column name strings,
                    apply to these columns. If a boolean, detect integer columns
                    and apply to these if True.
        format_float: should float formatting (fixed number of decimal places)
                      be applied? If a single column name string or list of
                      column name strings, apply to these columns. If a boolean,
                      detect float columns and apply to these if True.
        float_num_dec: number of decimal places to use in float
                       formatting.

        Returns a Styler object.
    """
    if isinstance(pdf, PDStyler):
        pdf_styler = pdf
        pdf_data = pdf.data
    elif isinstance(pdf, PDF):
        pdf_data = pdf
        pdf_styler = pdf.style
    else:
        raise ValueError("First arg must be either DataFrame or Styler.")

    def cols_from_arg(colarg):
        """ If explicit columns are given, return them as a list.

            Returns a non-empty list if the input is a non-empty string
            or list-like, otherwise an empty list.
        """
        if colarg and isinstance(colarg, basestring):
            return [colarg]
        if colarg and type(colarg) in (list, tuple):
            return list(colarg)
        return []

    ## Check if any formats were requested for specific columns.
    int_cols_arg = cols_from_arg(format_int)
    float_cols_arg = cols_from_arg(format_float)
    if int_cols_arg and float_cols_arg and \
            set(int_cols_arg).intersection(float_cols_arg):
        raise ValueError("If column names are given for both int and float" +
                         " formats, they cannot overlap.")

    def cols_to_format(include, exclude=[], numtype=None):
        """ Return a list of column names for a given number format.

            If specific columns to include were named, use that.
            Otherwise, detect columns of the given type, if any, and drop any
            columns listed to exclude.
        """
        if include:
            return include
        if numtype:
            cols_for_type = list(pdf_data.select_dtypes(include=[numtype]))
            return [coln for coln in cols_for_type if coln not in exclude]
        return []

    ## If specific columns were given for a format, use those.
    ## Otherwise, if formatting is requested, autodetect but exclude
    ## any specific columns listed for the other format.
    int_cols = cols_to_format(int_cols_arg, float_cols_arg,
                        "int" if format_int else None)
    float_cols = cols_to_format(float_cols_arg, int_cols_arg,
                        "float" if format_float else None)

    fmts = {}
    float_fmt = "{{:.{ndec}f}}".format(ndec=float_num_dec)
    for intcol in int_cols:
        fmts[intcol] = "{:,.0f}"
    for flcol in float_cols:
        fmts[flcol] = float_fmt
    return pdf_styler.format(fmts)


def pct_heatmap(pdf, adj=30, subset=None):
    """ Colors numeric cells containing percentages from red to green.

        pdf: a Pandas DataFrame or Styler object.
        adj: tune how far out into the ends of the color range to go
             (0 means from extreme to extreme)
        subset: the list of cols to which this should be applied
                (applies to all columns if None).

        Returns a DataFrame Styler object.
    """
    if isinstance(pdf, PDF):
        pdf_styler = pdf.style
    else:
        pdf_styler = pdf
    if not isinstance(pdf_styler, PDStyler):
        raise ValueError("Input must be either a DataFrame or a Styler.")

    norm = pltcolors.Normalize(-adj, 100+adj)
    def colour_pct_column(col):
        try:
            normed = norm(col.values)
            colmap = pltcmap("RdYlGn")
            hexcols = [pltcolors.rgb2hex(x) for x in colmap(normed)]
            return ["background-color: {}".format(color) for color in hexcols]
        except ValueError:
            return ["" for x in col.values]

    return pdf_styler.apply(colour_pct_column, subset=subset)


def pdf_shade_by_group(pdf, group_col, separate_tables=False):
    """ Shade Pandas DataFrame rows by alternate groups.

        Displays a df with rows shaded according to alternating values of a set
        of grouping columns, rather than shading alternate rows. Shading
        colours are currently hard-coded to be the same as in pandas_df.css.

        This function assumes that the df is already sorted by the grouping
        columns. Sorting is not done here to avoid problems with previously
        applied styles.

        pdf: a Pandas DataFrame or Styler object.
        group_col: a column name string of list of column names to group over.
        separate_tables: should groups be kept in separate dfs? If so, displays
                         a separate df for each group. Each df will be entirely
                         shaded the same colour. This may drop previously
                         applied styles.
    """
    if isinstance(pdf, PDStyler):
        pdf_styler = pdf
        pdf_data = pdf.data
    elif isinstance(pdf, PDF):
        pdf_data = pdf
        pdf_styler = pdf.style
    else:
        raise ValueError("First arg must be either a DataFrame or Styler.")
    if isinstance(group_col, basestring):
        group_col = [group_col]
    if type(group_col) not in (list, tuple):
        raise ValueError("Grouping columns must be specified either as a" +
                         " single string or list of strings.")
    grouped = pdf_data.groupby(group_col)
    group_keys = [key for key, gp in grouped]
    group_index = dict(zip(group_keys, range(len(group_keys))))

    def group_shading_str(group_key):
        row_gp_index = group_index[group_key]
        bgcol = "#f5fffa" if row_gp_index % 2 == 1 else "#fff"
        return "background-color: {}".format(bgcol)

    if separate_tables:
        for key, gp_df in grouped:
            shading_str = group_shading_str(key)
            group_styler = gp_df.style.apply(
                lambda row: [shading_str for x in row],
                axis=1)
            IPDisplay.display(group_styler)
    else:
        def shading_for_row(row):
            if len(group_col) == 1:
                row_gp_key = row[group_col[0]]
            else:
                row_gp_key = tuple([row[coln] for coln in group_col])
            shading_str = group_shading_str(row_gp_key)
            return [shading_str for x in row]

        IPDisplay.display(pdf_styler.apply(shading_for_row, axis=1))


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


def df_show_count_pct(pdf, n_overall=None, count_col="count",
                      order_by_count=False, show_cum_pct=False,
                      pct_col_name=None, fmt=True, pct_num_decimals=2):
    """ Format a Pandas DataFrame for displaying counts.
    
        Add percentages and format the numbers for printing. This does not
        modify the original df. If percentages are to be computed for multiple
        columns, the relevant args should be lists all of the same length.
        Optionally, the DataFrame can be ordered by decreasing count, and
        cumulative percentages can be added.

        n_overall: the denominator to use in computing the percentage. Supply
                   a single value or a list with one value for each count
                   column to compute percentages for, or a single None. The
                   value can be either:
                   - a number representing the total
                   - None, in which case the sum of the corresponding count
                     column is used (a single None can be given to apply this
                     to multiple count columns)
                   - a column name string, in which the count column is divided
                     by this column element-wise.
        count_col: the name of the count column for which percentages should be
                   computed. Supply a single string, or a list of strings to
                   compute percentages for multiple columns.
        pct_col_name: the name to use for the new percentage column. Supply a
                      single string or a list of strings for multiple percentage
                      columns. If None, each percentage column will be assigned
                      a default value.
        order_by_count: should the final df be ordered by decreasing count?
                        This should be either the string name of a count column
                        to order by, or a boolean indicating whether the df
                        should be ordered by the first count column.
        show_cum_pct: should cumulative percentages be added to the final table?
                      Note that this depends on the row order. Supply a single
                      string or a list of strings naming count columns for which
                      a cumulative percentage should be added, or a boolean
                      indicating whether cumulative percentages should be added
                      for all count columns. Default column naming is used.
        fmt: should display formatting be applied to the numeric columns? If
             True, returns a Styler object with number formatting applied to any
             count, n_overall, percentage and cumulative percentage columns.
        pct_num_decimals: number of decimal places to use for percentages when
                          applying formatting.

        Returns a DataFrame, or a Styler if formatting has been applied.
    """
    if isinstance(count_col, basestring):
        count_col = [count_col]
    if type(count_col) not in (list, tuple):
        raise ValueError("Count columns must be specified either as a single" +
                          " string or list of strings.")
    colsum = lambda coln: pdf[coln].sum()
    if not n_overall:
        n_overall = map(colsum, count_col)
    if isinstance(n_overall, basestring) or \
            type(n_overall) in (int, long, float):
        n_overall = [n_overall]
    if type(n_overall) not in (list, tuple):
        raise ValueError("Overall total must be either None, a single string" +
                         " or number, or a list of these.")
    if len(n_overall) != len(count_col):
        raise ValueError("Number of overall total columns must be the same as" +
                         " the number of count columns.")
    cols_n_overall = []
    for i, val in enumerate(n_overall):
        if not val:
            n_overall[i] = colsum(count_col[i])
        if isinstance(val, basestring):
            cols_n_overall.append(val)
            n_overall[i] = pdf[val]
    pct_name = lambda coln: "% {}".format(coln)
    if not pct_col_name:
        pct_col_name = map(pct_name, count_col)
    if isinstance(pct_col_name, basestring):
        pct_col_name = [pct_col_name]
    if type(pct_col_name) not in (list, tuple):
        raise ValueError("Names for percentage columns must be either None, a" +
                         " single string, or a list of these.")
    if len(pct_col_name) != len(count_col):
        raise ValueError("Number of percentage column names must be the same" +
                         " as the number of count columns.")
    for i, val in enumerate(pct_col_name):
        if not val:
            pct_col_name[i] = pct_name(count_col[i])
    if order_by_count == True:
        order_by_count = count_col[0]
    if isinstance(show_cum_pct, basestring):
        show_cum_pct = [show_cum_pct]
    if show_cum_pct == True:
        show_cum_pct = count_col

    if order_by_count and order_by_count in count_col:
        pdf = pdf.sort_values(order_by_count, ascending=False)
    else:
        pdf = pdf.copy()
    for i in range(len(count_col)):
        pdf[pct_col_name[i]] = pdf[count_col[i]] / n_overall[i] * 100
    cols_cum = []
    if show_cum_pct:
        for cumcol in show_cum_pct:
            try:
                cumcol_index = count_col.index(cumcol)
            except ValueError:
                continue
            cumcol_name = "cum {}".format(pct_col_name[cumcol_index])
            cumcol_ntot = n_overall[cumcol_index]
            pdf[cumcol_name] = pdf[cumcol].cumsum() / cumcol_ntot * 100
            cols_cum.append(cumcol_name)
    if not fmt:
        return pdf
    return fmt_pdf(pdf,
                  format_int=count_col + cols_n_overall,
                  format_float=pct_col_name + cols_cum,
                  float_num_dec=pct_num_decimals)


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
