import gradio as gr
import duckdb
import requests
import nest_asyncio

nest_asyncio.apply()

print("Starting Initialization...")

try:
    conn = duckdb.connect(database=":memory:")
    csv_file = "sales.csv"  
    conn.execute(f"CREATE TABLE sales AS SELECT * FROM read_csv_auto('{csv_file}')")
    print("DuckDB initialized and dataset loaded successfully.")
except Exception as e:
    print(f"Error initializing DuckDB or loading dataset: {str(e)}")

def get_table_schema(table_name):
    try:
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        schema = "\n".join([f"{row[0]} {row[1]}" for row in result])
        print(f"Fetched Schema for {table_name}:\n{schema}")
        return schema
    except Exception as e:
        error_message = f"Error fetching schema for {table_name}: {str(e)}"
        print(error_message)
        return None

def generate_sql_with_gemma2(prompt: str):
    try:
        print(f"Sending prompt to Gemma2: {prompt[:200]}...")  # Limit prompt logging for readability
        response = requests.post(
            "http://localhost:11434/api/generate",
            headers={"Content-Type": "application/json"},
            json={"model": "gemma2", "prompt": prompt, "stream": False}
        )
        print(f"Gemma2 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            query = response.json().get("response").strip()
            print(f"Generated Query: {query}")
            
            # Clean up the query
            query = query.replace("```sql", "").replace("```", "").strip()
            print(f"Cleaned Query: {query}")
            return query
        else:
            error_message = f"Error from Gemma2: {response.status_code}, {response.text}"
            print(error_message)
            return error_message
    except Exception as e:
        error_message = f"Error contacting Gemma2: {str(e)}"
        print(error_message)
        return error_message

def validate_query(query):
    try:
        print(f"Validating Query: {query}")
        conn.execute(f"EXPLAIN {query}")
        print("Query validation passed.")
        return True
    except Exception as e:
        error_message = f"Invalid SQL: {str(e)}"
        print(error_message)
        return error_message

def chatbot(user_query):
    try:
        print(f"User Query: {user_query}")

        table_name = "sales"
        schema = get_table_schema(table_name)
        if not schema:
            return f"Failed to fetch schema for table: {table_name}"

        prompt = f"""
        You are an AI SQL assistant. Generate only a SQL query (no explanations) based on the following table schema and natural language query.

        Table Schema:
        {table_name} (
        {schema}
        )

        Natural Language Query:
        {user_query}

        SQL Query:
        """
        generated_query = generate_sql_with_gemma2(prompt)
        if "Error" in generated_query:
            return generated_query

        validation_result = validate_query(generated_query)
        if validation_result is not True:
            return validation_result

        print(f"Executing Query: {generated_query}")
        result = conn.execute(generated_query).fetchall()
        print(f"Query Result: {result}")
        return f"Generated SQL:\n{generated_query}\n\nResults:\n{result}"
    except Exception as e:
        error_message = f"Error in chatbot function: {str(e)}"
        print(error_message)
        return error_message

print("Initializing Gradio Interface...")
gradio_interface = gr.Interface(
    fn=chatbot,
    inputs=gr.Textbox(lines=2, placeholder="Enter your query here..."),
    outputs="text",
    title="SQL Chatbot",
    description="Ask your natural language queries, and this chatbot will generate SQL and provide results."
)
print("Gradio Interface initialized successfully.")

if __name__ == "__main__":
    print("Launching Gradio...")
    try:
        gradio_interface.launch(share=True)
        print("Gradio launched successfully.")
    except Exception as e:
        print(f"Error launching Gradio: {str(e)}")
