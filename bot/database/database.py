import os
import motor.motor_asyncio # pylint: disable=import-error
from bot import DB_URI

DB_NAME = os.environ.get("DB_NAME", "Adv_Auto_Filter")

class Database:

    def __init__(self):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(DB_URI)
        self.db = self._client[DB_NAME]
        self.col = self.db["Main"]
        self.acol = self.db["Active_Chats"]
        self.fcol = self.db["Filter_Collection"]
        
        self.cache = {}
        self.acache = {}


    async def create_index(self):
        """
        Create text index if not in db
        """
        await self.fcol.create_index([("file_name", "text")])


    def new_chat(self, group_id, channel_id, channel_name):
        """
        Create a document in db if the chat is new
        """
        try:
            group_id, channel_id = int(group_id), int(channel_id)
        except:
            pass
        
        return dict(
            _id = group_id,
            chat_ids = [{
                "chat_id": channel_id,
                "chat_name": channel_name
                }],
            types = dict(
                audio=False,
                document=True,
                video=True
            ),
            configs = dict(
                accuracy=0.80,
                max_pages=5,
                max_results=50,
                max_per_page=10,
                pm_fchat=True,
                show_invite_link=True
            )
        )


    async def status(self, group_id: int):
        """
        Get the total filters, total connected
        chats and total active chats of a chat
        """
        group_id = int(group_id)
        
        total_filter = await self.tf_count(group_id)
        
        chats = await self.find_chat(group_id)
        chats = chats.get("chat_ids")
        total_chats = len(chats) if chats is not None else 0
        
        achats = await self.find_active(group_id)
        if achats not in (None, False):
            achats = achats.get("chats")
            if achats == None:
                achats = []
        else:
            achats = []
        total_achats = len(achats)
        
        return total_filter, total_chats, total_achats


    async def find_group_id(self, channel_id: int):
        """
        Find all group id which is connected to a channel 
        for add a new files to db
        """
        data = self.col.find({})
        group_list = []

        for group_id in await data.to_list(length=50): # No Need Of Even 50
            for y in group_id["chat_ids"]:
                if int(y["chat_id"]) == int(channel_id):
                    group_list.append(group_id["_id"])
                else:
                    continue
        return group_list

    # Related TO Finding Channel(s)
    async def find_chat(self, group_id: int):
        """
        A funtion to fetch a group's settings
        """
        connections = self.cache.get(str(group_id))
        
        if connections is not None:
            return connections

        connections = await self.col.find_one({'_id': group_id})
        
        if connections:
            self.cache[str(group_id)] = connections

            return connections
        else: 
            return self.new_chat(None, None, None)

        
    async def add_chat(self, group_id: int, channel_id: int, channel_name):
        """
        A funtion to add/update a chat document when a new chat is connected
        """
        new = self.new_chat(group_id, channel_id, channel_name)
        update_d = {"$push" : {"chat_ids" : {"chat_id": channel_id, "chat_name" : channel_name}}}
        prev = await self.col.find_one({'_id':group_id})
        
        if prev:
            await self.col.update_one({'_id':group_id}, update_d)
            await self.update_active(group_id, channel_id, channel_name)
            await self.refresh_cache(group_id)
            
            return True
        
        self.cache[str(group_id)] = new
        
        await self.col.insert_one(new)
        await self.add_active(group_id, channel_id, channel_name)
        await self.refresh_cache(group_id)
        
        return True


    async def del_chat(self, group_id: int, channel_id: int):
        """
        A Funtion to delete a channel and its files from db of a chat connection
        """
        group_id, channel_id = int(group_id), int(channel_id) # group_id and channel_id Didnt type casted to int for some reason
        
        prev = self.col.find_one({"_id": group_id})
        
        if prev:
            
            await self.col.update_one(
                {"_id": group_id}, 
                    {"$pull" : 
                        {"chat_ids" : 
                            {"chat_id":
                                channel_id
                            }
                        }
                    },
                False,
                True
            )

            await self.del_active(group_id, channel_id)
            await self.refresh_cache(group_id)

            return True

        return False


    async def in_db(self, group_id: int, channel_id: int):
        """
        Check whether if the given channel id is in db or not...
        """
        connections = self.cache.get(group_id)
        
        if connections is None:
            connections = await self.col.find_one({'_id': group_id})
        
        check_list = []
        
        if connections:
            for x in connections["chat_ids"]:
                check_list.append(int(x.get("chat_id")))

            if int(channel_id) in check_list:
                return True
        
        return False
