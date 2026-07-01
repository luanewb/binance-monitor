FROM python:3.10-slim

WORKDIR /app

# Install system dependencies if any are needed (none for now)
# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code
COPY . .

# Expose port
EXPOSE 8080

# Command to run the dashboard
CMD ["uvicorn", "dashboard:app", "--host", "0.0.0.0", "--port", "8080"]
