import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

# Replace with your Azure Blob Storage connection string and container name
connection_string = "DefaultEndpointsProtocol=https;AccountName=narula12storage;AccountKey=s8rUHL11ngvXxzJMatsIPT1UKaQsXMw61lKTTb7xA4bM2AawsFIpuf0I4Ty5rwsPpqg4t6IDGe6c+AStCavGIg==;EndpointSuffix=core.windows.net"
container_name = "testcontainer"

def load_blob_files_to_dataframe():
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # Initialize an empty DataFrame
    combined_df = pd.DataFrame()

    # List all blobs in the container
    blob_list = container_client.list_blobs()

    for blob in blob_list:
        if blob.name.endswith('.csv'):  # Adjust based on your file type
            print(f"Loading {blob.name}...")
            # Download the blob content into a stream
            blob_client = container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob()
            # Read the content of the blob into a DataFrame
            df = pd.read_csv(io.BytesIO(blob_data.readall()))
            # Append to combined DataFrame
            combined_df = pd.concat([combined_df, df], ignore_index=True)

    return combined_df

@app.get("/load-data")
async def load_data():
    try:
        dataframe = load_blob_files_to_dataframe()
        # Convert the DataFrame to JSON
        result = dataframe.to_json(orient='records')
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
