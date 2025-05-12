from filebrowser_client import FilebrowserClient
import asyncio

def filebrowseraction(file=None,args="up"):
    async def connect(file, args):
        client = FilebrowserClient("http://localhost:8080", "admin", "admin")
        await client.connect()
        print("connected")
        try:
            if file and args == "up":
                await client.upload(local_path=file, remote_path="SLA_GAME/")
                print(f"Uploaded: {file}")
            elif args == "del":

                path = f"/SLA_GAME"
                await client.delete(path)
                print(f"Deleted:{path}")
        except Exception as e:
            print(e)
    asyncio.run(connect(file,args))

SMTP_SERVER = "smtp.gmail.com"  # Replace with your SMTP server
SMTP_PORT = 465  # Use 465 for SSL or 587 for TLS
USERNAME = "ngtrdung240101@gmail.com"  # Your email login
PASSWORD = "your_password"  # Your email password
if __name__=="__main__":
    filebrowseraction("del")
