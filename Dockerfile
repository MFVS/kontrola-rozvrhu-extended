FROM python:3.12.2

WORKDIR /app

COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 8501

# Define environment variable
ENV NAME World

CMD ["streamlit", "run", "app_ps.py"]