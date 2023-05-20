"""Helper functions when using gcs storage

 Coppied from https://cloud.google.com/community/tutorials/cloud-storage-infinite-compose
"""
import time
import logging
from typing import List, Any, Iterable
from concurrent.futures import Executor, Future
from itertools import count
from google.cloud import storage


def compose(
    object_path: str,
    slices: List[storage.Blob],
    client: storage.Client,
    executor: Executor,
    log: logging.Logger,
) -> storage.Blob:
    """Compose an object from an indefinite number of slices. Composition is
    performed concurrently using a tree of accumulators. Cleanup is
    performed concurrently using the provided executor.

    Arguments:
        object_path {str} -- The path for the final composed blob.
        slices {List[storage.Blob]} -- A list of the slices that should
            compose the blob, in order.
        client {storage.Client} -- A Cloud Storage client to use.
        executor {Executor} -- A concurrent.futures.Executor to use for
            cleanup execution.

    Returns:
        storage.Blob -- The composed blob.
    """
    log.info("Composing")

    chunks = generate_composition_chunks(slices)
    next_chunks = []
    identifier = generate_hex_sequence()

    while len(next_chunks) > 32 or not next_chunks:  # falsey empty list is ok
        for chunk in chunks:
            # make intermediate accumulator
            intermediate_accumulator = storage.Blob.from_string(
                object_path + next(identifier)
            )
            log.info("Intermediate composition: %s", intermediate_accumulator)
            future_iacc = executor.submit(
                compose_and_cleanup,
                intermediate_accumulator,
                chunk,
                client,
                executor,
                log,
            )
            # store reference for next iteration
            next_chunks.append(future_iacc)
        # go again with intermediate accumulators
        chunks = generate_composition_chunks(next_chunks)

    # Now can do final compose
    final_blob = storage.Blob.from_string(object_path)
    # chunks is a list of lists, so flatten it
    final_chunk = [blob for sublist in chunks for blob in sublist]
    compose_and_cleanup(final_blob, final_chunk, client, executor, log)

    log.info("Composition complete")

    return final_blob


def compose_and_cleanup(
    blob: storage.Blob,
    chunk: List[storage.Blob],
    client: storage.Client,
    executor: Executor,
    log: logging.Logger,
):
    """Compose a blob and clean up its components. Cleanup tasks are
    scheduled in the provided executor and the composed blob immediately
    returned.

    Args:
        blob (storage.Blob): The blob to be composed.
        chunk (List[storage.Blob]): The component blobs.
        client (storage.Client): A Cloud Storage client.
        executor (Executor): An executor in which to schedule cleanup tasks.

    Returns:
        storage.Blob: The composed blob.
    """
    # wait on results if the chunk has any futures
    chunk = ensure_results(chunk)
    blob.compose(chunk, client=client)
    # clean up components, no longer need them
    delete_objects_concurrent(chunk, executor, client, log)
    return blob


def ensure_results(maybe_futures: List[Any]) -> List[Any]:
    """Pass in a list that may contain futures, and if so, wait for
    the result of the future; for all other types in the list,
    simply append the value.

    Args:
        maybe_futures (List[Any]): A list which may contain futures.

    Returns:
        List[Any]: A list with the values passed in, or Future.result() values.
    """
    results = []
    for m_res in maybe_futures:
        if isinstance(m_res, Future):
            results.append(m_res.result())
        else:
            results.append(m_res)
    return results


def generate_hex_sequence() -> Iterable[str]:
    """Generate an indefinite sequence of hexadecimal integers.

    Yields:
        Iterator[Iterable[str]]: The sequence of hex digits, as strings.
    """
    for i in count(0):
        yield hex(i)[2:]


def generate_composition_chunks(slices: List, chunk_size: int = 31) -> Iterable[List]:
    """Given an indefinitely long list of blobs, return the list in 31-item chunks.

    Arguments:
        slices {List} -- A list of blobs, which are slices of a desired final blob.

    Returns:
        Iterable[List] -- An iteration of 31-item chunks of the input list.

    Yields:
        Iterable[List] -- A 31-item chunk of the input list.
    """
    while len(slices):
        chunk = slices[:chunk_size]
        yield chunk
        slices = slices[chunk_size:]


def delete_objects_concurrent(blobs, executor, client, log: logging.Logger) -> None:
    """Delete Cloud Storage objects concurrently.

    Args:
        blobs (List[storage.Blob]): The objects to delete.
        executor (Executor): An executor to schedule the deletions in.
        client (storage.Client): Cloud Storage client to use.
    """
    for blob in blobs:
        log.debug("Deleting slice %s", blob.name)
        executor.submit(blob.delete, client=client)
        time.sleep(0.005)  # quick and dirty ramp-up (Sorry, Dijkstra.)
