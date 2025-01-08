from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from typing import List, Optional, Set
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QdrantCollectionMerger:
    def __init__(self, host: str = "{yourIP}", port: int = 6333):
        self.client = QdrantClient(host=host, port=port)

    def get_existing_ids(self, collection_name: str) -> Set[str]:

        existing_ids = set()
        scroll_filter = None
        batch_size = 1000  # Larger batch size for faster ID retrieval

        while True:
            records, next_page_offset = self.client.scroll(
                collection_name=collection_name,
                limit=batch_size,
                offset=scroll_filter,
                with_vectors=False, 
                with_payload=False 
            )
            
            if not records:
                break
                
            existing_ids.update(str(record.id) for record in records)
            scroll_filter = next_page_offset
            
            if scroll_filter is None:
                break
                
        logger.info(f"Found {len(existing_ids)} existing points in {collection_name}")
        return existing_ids

    def merge_collections(self,
                         source_collections: List[str],
                         target_collection: str,
                         vector_size: Optional[int] = None,
                         resume: bool = True):

        existing_collections = self.client.get_collections().collections
        existing_names = [col.name for col in existing_collections]

        for col in source_collections:
            if col not in existing_names:
                raise ValueError(f"Source collection {col} does not exist")

        if target_collection not in existing_names:
            if vector_size is None:
                vector_size = self.client.get_collection(source_collections[0]).config.params.vectors.size

            logger.info(f"Creating target collection {target_collection}")
            self.client.create_collection(
                collection_name=target_collection,
                vectors_config={"size": vector_size, "distance": "Cosine"}
            )

        existing_ids = self.get_existing_ids(target_collection) if resume else set()

        for source_col in source_collections:
            logger.info(f"Processing collection: {source_col}")

            batch_size = 100
            points_processed = 0
            points_skipped = 0
            scroll_filter = None

            while True:
                records, next_page_offset = self.client.scroll(
                    collection_name=source_col,
                    limit=batch_size,
                    offset=scroll_filter,
                    with_vectors=True,
                    with_payload=True
                )

                if not records:
                    break

                points = []
                for record in records:
                   
                    if str(record.id) in existing_ids:
                        points_skipped += 1
                        continue

                    payload = record.payload or {}
                    payload['source_collection'] = source_col

                    if record.vector is not None:
                        point = PointStruct(
                            id=record.id,
                            vector=record.vector,
                            payload=payload
                        )
                        points.append(point)
                        existing_ids.add(str(record.id))
                    else:
                        logger.warning(f"Skipping point {record.id} - no vector data")

                if points:
                    self.client.upsert(
                        collection_name=target_collection,
                        points=points
                    )

                points_processed += len(records)
                logger.info(f"Processed {points_processed} points from {source_col} "
                          f"(skipped {points_skipped} existing points)")

                scroll_filter = next_page_offset
                if scroll_filter is None:
                    break

            logger.info(f"Completed merging collection: {source_col} - "
                      f"Total processed: {points_processed}, Total skipped: {points_skipped}")

        logger.info("Merge operation completed successfully")

if __name__ == "__main__":
    merger = QdrantCollectionMerger()
    merger.merge_collections(
        source_collections=[
            "{your multiple collections here}",
            "{your multiple collections here}"
        ],
        target_collection="{target collection}",
        vector_size=3072,
        resume=True  # Enable Resuming from last point
    )