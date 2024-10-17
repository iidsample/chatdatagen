import time
import json
import numpy as np
import random
import pdb

import asyncio
import grpc
import chat_pb2
import chat_pb2_grpc
import threading



def blocking_sleep(sleep_time):
        time.sleep(sleep_time)

async def non_blocking_sleep(sleep_time):
        await asyncio.sleep(sleep_time)

# class ChatLoader:
#     req_counter = 0
    
async def rpc_call(client_id):
    """
    RPC calls for sending requests to the server
    """
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        
        stub = chat_pb2_grpc.LlmEngineStub(channel)
        input = "No, asyncio itself doesn't use multiple threads. Instead, it lets you do lots of things at once using a single-thread approach, which is like having a super-efficient to-do list for your computer. However, you can mix it with threads using special functions like asyncio. to_thread or asyncio. Why is that?"
        # print("This is the input sent: ",input)
        new_req_id = str(client_id)+"req"
        
        
        request = chat_pb2.ChatReq(
            prompt=input, request_id=new_req_id, session_id=client_id
        )
        # pdb.set_trace()
        print(f"time: {time.time()}")
        print(f"Ready to send request with id {new_req_id}, and session id {client_id} with input {input[:100]}!")
        
        response = await stub.processChatReq(request)
        print("Receive llm text: ", response.answer[:100])
        # pdb.set_trace()
        
def task_thread(loop, client_id):
    # report a message
    print('thread running')
    # wait a moment
    # create a coroutine
    coro = rpc_call(client_id)
    # execute a coroutine
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    # wait for the task to finish
    future.result()
    # report a message
    print('thread done')

async def send_data():
    # chatLoader = ChatLoader()
    PATH = "/home/anyong/Downloads/sg_90k_part1.json"
    with open(PATH, "r") as fopen:
        open_data = json.load(fopen)
    new_clients = {
                # may be set to max(client_id) + i
                # max_key + i: self.open_data.pop(0)["conversations"]
                i: open_data.pop(0)["conversations"]
                for i in range(10)
            }
    task_list = []
    loop = asyncio.get_running_loop()
    for i in range(10):
        print(f"time: {time.time()}")
        # await asyncio.to_thread(blocking_sleep, 5)
        # start a new thread
        threading.Thread(target=task_thread, args=(loop,i)).start()
        # task = asyncio.create_task(chatLoader.rpc_call(new_clients[i][0], i))
        # task_list.append(task)
        # await asyncio.to_thread(blocking_sleep, 2)
        time_to_wait = 2
        if i==9:
            time_to_wait = 150 
        
        await asyncio.sleep(time_to_wait)
        # asyncio.create_task(non_blocking_sleep(2))

if __name__ == "__main__":
    asyncio.run(send_data())