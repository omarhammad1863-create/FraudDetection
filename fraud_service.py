"""Evaluate an amount and its customer's recent attempt count over gRPC."""

import asyncio
import os

import grpc

import fraud_pb2
import fraud_pb2_grpc


class FraudDetector(fraud_pb2_grpc.FraudDetectorServicer):
    async def Evaluate(self, request, context):
        if request.amount_cents <= 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Amount must be positive")
        if request.tx_count_5m <= 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Attempt count must be positive")
        decision = (
            "BLOCK"
            if request.amount_cents > 1_000_000 or request.tx_count_5m >= 4
            else "ALLOW"
        )
        return fraud_pb2.FraudResponse(decision=decision)


async def serve():
    server = grpc.aio.server()
    fraud_pb2_grpc.add_FraudDetectorServicer_to_server(FraudDetector(), server)
    address = os.getenv("FRAUD_BIND", "127.0.0.1:50051")
    server.add_insecure_port(address)
    await server.start()
    print(f"Fraud service listening on {address}", flush=True)
    try:
        await server.wait_for_termination()
    finally:
        await server.stop(grace=1)


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass
