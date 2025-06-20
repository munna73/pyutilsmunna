import sys
import os

# Add project root (2 levels up) to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from db.database import Database  # Now this works

def main():
    db = Database('config.ini')

    # Get the query from section/key
    query = db.get_query('queries', 'emp_summary')

    if not query:
        print("Query not found.")
        return

    # Run the query
    df = db.run_query(query)

    # Show results
    if not df.empty:
        print("Query Results:")
        print(df.head())
    else:
        print("No data returned or query failed.")

if __name__ == '__main__':
    main()