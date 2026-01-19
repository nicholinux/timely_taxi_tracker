import duckdb

@st.cache_resource
def get_connection():
    return duckdb.connect("analytics.duckdb")

con = get_connection()

df = con.execute("query_goes_here").df()
st.line_chart(df)
