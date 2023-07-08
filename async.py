import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import Base, Session, SWpeople, engine


MAX_CHUNK_SIZE = 10

async def get_person(person_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{person_id}")
    json_data = await response.json()
    await session.close()
    return json_data

async def get_details(list_details):
    session  = aiohttp.ClientSession()
    dicts = await asyncio.gather(list_details)
    get_names_details = []
    for dict in dicts:
        for keys, values in dict.items():
            if type(values) == list:
                for name in values:
                    responce = await session.get(f"{name}")
                    get_names_detail = await responce.json()
                    get_names_details.append(get_names_detail[list(get_names_detail.keys())[0]])
                dict[keys] = ", ".join(get_names_details)
                get_names_details = []
            elif "https://" in values:
                responce = await session.get(f"{values}")
                get_names_detail = await responce.json()
                dict[keys] = get_names_detail[list(get_names_detail.keys())[0]]
    await session.close()
    return dict

async def insert_to_db(person_list):
    async with Session() as session:
        for json_data in person_list:
            if "detail" not in json_data:
                swars_list = [
                    SWpeople (
                                birth_year = json_data["birth_year"],
                                eye_color = json_data["eye_color"],
                                films = json_data["films"],
                                gender = json_data["gender"],
                                hair_color = json_data["hair_color"],
                                height = json_data["height"],
                                homeworld = json_data["homeworld"],
                                mass = json_data["mass"],
                                name = json_data["name"],
                                skin_color = json_data["skin_color"],
                                species = json_data["species"],
                                starships = json_data["starships"],
                                vehicles = json_data["vehicles"]
                    )
                ]
                session.add_all(swars_list)
                await session.commit()

async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    for ids_chunk in chunked(range(1, 91), MAX_CHUNK_SIZE):
        get_person_coros = [get_details(get_person(person_id)) for person_id in ids_chunk]
        person_list = await asyncio.gather(*get_person_coros)
        asyncio.create_task(insert_to_db(person_list))

    current_task = asyncio.current_task()
    tasks_sets = asyncio.all_tasks()
    tasks_sets.remove(current_task)

    await asyncio.gather(*tasks_sets)
    await engine.dispose()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)