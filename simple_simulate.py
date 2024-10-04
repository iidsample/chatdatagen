import time
import asyncio

"""This file shows how to make time.sleep non blocking for async function in background().
By using asyncio.to_thread(), it successfully avoids the loop stucking on time.sleep() before
calling the async function background().

"""

# blocking function
def blocking_task():
    # report a message
    
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('task is running',current_time)
    # block
    time.sleep(1)
    # report a message
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('task is done',current_time)
 
# background coroutine task
async def background():
    # loop forever
    # while True:
    # report a message
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('>background task running', current_time)
    # sleep for a moment
    await asyncio.sleep(1.5)
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('>background task finished', current_time)
 
# main coroutine
async def main():
    task_list = []
    for i in range(20):
        t = time.localtime()
        current_time = time.strftime("%H:%M:%S", t)
        print('Loop is running',current_time)
        task = asyncio.create_task(background())
        task_list.append(task)
        coro = asyncio.to_thread(blocking_task)
        await coro
    # # run the background task
    # _= asyncio.create_task(background())
    # create a coroutine for the blocking function call
    # coro = asyncio.to_thread(blocking_task)
    # execute the call in a new thread and await the result
    # await coro

if __name__ == "__main__":
    # start the asyncio program
    asyncio.run(main())