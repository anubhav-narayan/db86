"""
Launcher script for DB86 Local REST Server.
Runs the FastAPI service on local host (127.0.0.1:8000 by default).
"""
import sys
import os
import argparse
import uvicorn

# Ensure the db86 package directory is on python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description="DB86 Local REST Server Runner")
    parser.add_argument("--host", default="127.0.0.1", help="Host address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    parser.add_argument("--reload", action="store_true", default=True, help="Enable auto-reload")
    args = parser.parse_args()

    print(f"=================================================")
    print(f"  DB86 Local Database REST Server")
    print(f"  Listening on: http://{args.host}:{args.port}")
    print(f"  Swagger Docs: http://{args.host}:{args.port}/docs")
    print(f"=================================================")

    uvicorn.run(
        "db86.service.rest_service:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )

if __name__ == "__main__":
    main()
