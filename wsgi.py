from IndianMedia.web.main import app
import os

if __name__ == "__main__":
    app.run(debug=True , port=os.environ.get("PORT" , 5000))

    import psutil
    process = psutil.Process(os.getpid())
    print(process.memory_info().rss / 10**6)
