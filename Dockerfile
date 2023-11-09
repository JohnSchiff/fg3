# Use the official Python image as the base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the application code into the container
COPY . /app

# Install dependencies
RUN pip install -r requirements.txt

# Expose the port on which the Streamlit app will run
EXPOSE 8501

# Define the command to run the Streamlit app
CMD ["streamlit", "run", "main.py"]

