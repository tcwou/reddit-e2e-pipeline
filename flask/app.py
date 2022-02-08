# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from reddit_to_postgres import RedditToPostgres

server = flask.Flask(__name__)

app = dash.Dash(__name__, routes_pathname_prefix='/', requests_pathname_prefix='/stocks/')
app.title = 'Stock Investing Subreddits Dashboard'

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options

rtp = RedditToPostgres(
    config_path='./configs/extract_data_config.json'
)

total_query = """
SELECT
    date,
    SUM(posts_1d) AS posts_1d,
    SUM(upvotes_1d) AS upvotes_1d,
    SUM(comments_1d) AS comments_1d,
    SUM(awards_1d) AS awards_1d,
    SUM(authors_1d) AS authors_1d,
    CASE 
        WHEN SUM(sent_weight_denom_1d) < 1 THEN NULL
        ELSE ROUND(CAST(SUM(sent_weight_num_1d) * 1.0 / SUM(sent_weight_denom_1d) AS numeric), 3)
    END AS sentiment_1d
FROM public.subreddit_aggregate
GROUP BY date
ORDER BY date
"""

total_data = rtp.postgres_get(total_query)

top_tickers_query = """
    SELECT
        symbol,
        upvotes_7d,
        mentions_7d,
        comments_7d,
        awards_7d,
        ROUND(sentiment_7d::numeric, 3) AS sentiment_7d,
        mentions_sparkline
    FROM (
        SELECT 
            *,
            RANK() OVER(ORDER BY upvotes_7d DESC) AS r
        FROM (
            SELECT
              p.symbol,
              SUM(p.upvotes_1d) AS upvotes_7d,
              SUM(p.mentions_1d) AS mentions_7d,
              SUM(p.comments_1d) AS comments_7d,
              SUM(p.awards_1d) AS awards_7d,
                CASE 
                    WHEN SUM(sent_weight_denom_1d) < 1 THEN NULL
                    ELSE SUM(sent_weight_num_1d) * 1.0 / SUM(sent_weight_denom_1d)
                END AS sentiment_7d,
              ARRAY_AGG(p.mentions_1d) AS mentions_sparkline
            FROM (
                SELECT 
                    a.date, 
                    b.symbol, 
                    sym.upvotes_1d, 
                    COALESCE(sym.mentions_1d, 0) AS mentions_1d, 
                    sym.comments_1d, 
                    sym.awards_1d,
                    sent_weight_num_1d,
                    sent_weight_denom_1d
                FROM (SELECT DISTINCT date FROM public.subreddit_symbol_aggregate) a 
                CROSS JOIN (SELECT DISTINCT symbol FROM public.subreddit_symbol_aggregate) b
                LEFT JOIN (
                    SELECT
                        date,
                        symbol,
                        SUM(upvotes_1d) AS upvotes_1d,
                        SUM(mentions_1d) AS mentions_1d,
                        SUM(comments_1d) AS comments_1d,
                        SUM(awards_1d) AS awards_1d,
                        SUM(sent_weight_num_1d) AS sent_weight_num_1d,
                        SUM(sent_weight_denom_1d) AS sent_weight_denom_1d   
                    FROM public.subreddit_symbol_aggregate
                    GROUP BY date, symbol
                ) sym ON sym.date = a.date AND sym.symbol = b.symbol
                WHERE a.date BETWEEN
                    (SELECT MAX(date) - 7 FROM public.subreddit_symbol_aggregate)
                    AND (SELECT MAX(date) FROM public.subreddit_symbol_aggregate)
            ) p
            GROUP BY p.symbol
        ) agg
        WHERE agg.upvotes_7d IS NOT NULL
    ) agg_ranked
    WHERE r <= 10
"""

top_tickers_data = rtp.postgres_get(top_tickers_query)

top_tickers_subreddit_query = """
SELECT
    subreddit,
    symbol,
    upvotes_7d,
    mentions_7d,
    comments_7d,
    awards_7d,
    ROUND(sentiment_7d::numeric, 3) AS sentiment_7d
FROM(
    SELECT 
        *,
        RANK() OVER(PARTITION BY subreddit ORDER BY upvotes_7d DESC) AS r
    FROM (
        SELECT
            subreddit,
            symbol,
            SUM(upvotes_1d) AS upvotes_7d,
            SUM(mentions_1d) AS mentions_7d,
            SUM(comments_1d) AS comments_7d,
            SUM(awards_1d) AS awards_7d,
            CASE 
                WHEN SUM(sent_weight_denom_1d) < 1 THEN NULL
                ELSE SUM(sent_weight_num_1d) * 1.0 / SUM(sent_weight_denom_1d)
            END AS sentiment_7d
        FROM public.subreddit_symbol_aggregate
        WHERE date BETWEEN
            (SELECT MAX(date) - 7 FROM public.subreddit_symbol_aggregate)
            AND (SELECT MAX(date) FROM public.subreddit_symbol_aggregate)
        AND symbol IS NOT NULL
        GROUP BY subreddit, symbol
    ) agg
) agg_ranked
WHERE r <= 10
"""

top_tickers_subreddit_data = rtp.postgres_get(top_tickers_subreddit_query)

sr_agg_cols_query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = 'subreddit_aggregate';
"""
sr_agg_query = """
SELECT
    date,
    subreddit,
    posts_1d,
    upvotes_1d,
    comments_1d,
    upvotes_1d * 1.0 / posts_1d AS avg_upvotes_1d,
    avg_comments_1d,
    authors_1d,
    ROUND(sentiment_1d::numeric, 3) AS sentiment_1d
FROM public.subreddit_aggregate
ORDER BY date, subreddit;
"""

sr_agg_data = rtp.postgres_get(sr_agg_query)

sr_agg_cols = ['date', 'subreddit', 'posts_1d', 'upvotes_1d', 'comments_1d', 'avg_upvotes_1d', 'avg_comments_1d', 'authors_1d', 'sentiment_1d']
total_cols = ['date', 'posts_1d', 'upvotes_1d', 'comments_1d', 'awards_1d', 'authors_1d', 'sentiment_1d']
ticker_cols = ['Symbol', 'Post Upvotes', 'Post Mentions', 'Post Comments', 'Post Awards', 'Average Sentiment', 'Post Mentions Sparkline']
sr_ticker_cols = ['Subreddit'] + ticker_cols[:-1]


# data
df_total = pd.DataFrame(total_data, columns=total_cols)
df_top_tickers = pd.DataFrame(top_tickers_data, columns=ticker_cols)
df_sr_agg = pd.DataFrame(sr_agg_data, columns=sr_agg_cols)
df_tickers_sr = pd.DataFrame(top_tickers_subreddit_data, columns=sr_ticker_cols)

# sparkline
df_top_tickers['Post Mentions Sparkline'] = [str(l[0]) + '{' + ','.join(map(str, l)) + '}' + str(l[-1]) 
                                                for l in df_top_tickers['Post Mentions Sparkline']]

# overview static figures
total_posts = px.line(df_total, x="date", y="posts_1d", height=350)
total_comments = px.line(df_total, x="date", y="comments_1d", height=350)
total_sentiment = px.line(df_total, x="date", y="sentiment_1d", height=350)

# dropdowns
sr_opts = [{'label' : sr, 'value' : sr} for sr in df_sr_agg['subreddit'].unique()]

app.layout = html.Div([
    html.Link(rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/font-awesome/4.4.0/css/font-awesome.min.css"),
    html.Div(
        className="app-header app-header-banner",
        children=[
            html.Div('Stock Investing Subreddits Dashboard', className="app-header--title dotline-extrathick"),
            html.A(html.I(className="fa fa-github"),
                href="https://github.com/tcwou", target="_blank", style={'float': 'right',
                 'color': 'white', 'margin-top': '6px', 'margin-right': '30px'})
        ]
    ),
    html.Div(
        className="app-section",
        children=[
            html.H4('Overall', className="app-header dotline-extrathick")
        ]
    ),
    html.Div([
        html.Div([
            html.H5('Top Tickers (7 days)'),
            dash_table.DataTable(id='t1',
                columns=[{"name": i, "id": i} 
                    for i in df_top_tickers.columns],
                data=df_top_tickers.to_dict('records'),
                style_cell=dict(textAlign='left'),
                style_data_conditional=[{"if": {"column_id": "Post Mentions Sparkline"}, "font-family": "Sparks-Bar-Wide",}])

        ], className="twelve columns pretty_container dotline-extrathick"),
    ], className="row"),
    html.Div([
        html.Div([
            html.H5('Posts'),
            dcc.Graph(id='total-posts', figure=total_posts)
        ], className="four columns pretty_container dotline-extrathick"),

        html.Div([
            html.H5('Comments'),
            dcc.Graph(id='total-comments', figure=total_comments)
        ], className="four columns pretty_container dotline-extrathick"),

        html.Div([
            html.H5('Sentiment'),
            dcc.Graph(id='total-sentiment', figure=total_sentiment)
        ], className="four columns pretty_container dotline-extrathick"),
    ], className="row"),
    html.Div(
        className="app-section",
        children=[
            html.H4('Subreddits', className="app-header dotline-extrathick")
        ]
    ),
    html.Div([
        html.P([
            html.Label("Choose a Subreddit"),
            dcc.Dropdown(id = 'subreddit_opt', 
                         options = sr_opts,
                         value = 'investing')
                ], style = {'width': '400px',
                            'fontSize' : '20px',
                            'padding-left' : '100px',
                            'display': 'inline-block'}),
    ], className="row"),

    html.Div([
        html.H5('Top Tickers (7 days)'),
        html.Div(id='sr-tickers')
    ], className="twelve columns pretty_container dotline-extrathick"),

    html.Div([
        html.Div([
            html.H5('Posts'),
            dcc.Graph(id='sr-posts')
        ], className="four columns pretty_container dotline-extrathick"),
        html.Div([
            html.H5('Comments'),
            dcc.Graph(id='sr-comments')
        ], className="four columns pretty_container dotline-extrathick"),
        html.Div([
            html.H5('Sentiment'),
            dcc.Graph(id='sr-sentiment')
        ], className="four columns pretty_container dotline-extrathick"),
    ], className="row"),
    html.Div([
        html.Div([
            html.H5('Avg upvotes per post'),
            dcc.Graph(id='sr-upvotes-per-post')
        ], className="four columns pretty_container dotline-extrathick"),
        html.Div([
            html.H5('Avg comments per post'),
            dcc.Graph(id='sr-comments-per-post')
        ], className="four columns pretty_container dotline-extrathick"),
        html.Div([
            html.H5('Unique posters'),
            dcc.Graph(id='sr-authors')
        ], className="four columns pretty_container dotline-extrathick"),
    ], className="row"),
    
])


@app.callback(
    Output('sr-tickers', 'children'),
    Output('sr-posts', 'figure'),
    Output('sr-comments', 'figure'),
    Output('sr-sentiment', 'figure'),
    Output('sr-upvotes-per-post', 'figure'),
    Output('sr-comments-per-post', 'figure'),
    Output('sr-authors', 'figure'),
    Input('subreddit_opt', 'value'))
def update_graph(subreddit_value):

    df_sr_filter = df_sr_agg[df_sr_agg['subreddit'] == subreddit_value]

    df_tickers_sr_filter = df_tickers_sr[df_tickers_sr['Subreddit'] == subreddit_value]

    sr_tickers = html.Div([
        dash_table.DataTable(
        columns=[{"name": i, "id": i} 
            for i in df_tickers_sr_filter.columns if i != 'Subreddit'],
        data=df_tickers_sr_filter.to_dict('records'),
        style_cell=dict(textAlign='left'))
    ])

    sr_posts = px.line(df_sr_filter, x="date", y="posts_1d", height=350)

    sr_comments = px.line(df_sr_filter, x="date", y="comments_1d", height=350)

    sr_sentiment = px.line(df_sr_filter, x="date", y="sentiment_1d", height=350)

    sr_upvotes_per_post = px.line(df_sr_filter, x="date", y="avg_upvotes_1d", height=350)

    sr_comments_per_post = px.line(df_sr_filter, x="date", y="avg_comments_1d", height=350)

    sr_authors = px.line(df_sr_filter, x="date", y="authors_1d", height=350)

    return sr_tickers, sr_posts, sr_comments, sr_sentiment, sr_upvotes_per_post, sr_comments_per_post, sr_authors

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')
