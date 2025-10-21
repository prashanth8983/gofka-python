#!/bin/bash

# Quick launcher for Gofka Python interactive examples

echo "╔════════════════════════════════════════════════════════════╗"
echo "║        Gofka Python Interactive Examples Launcher         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check if broker is specified
BROKER="${1:-localhost:9092}"
TOPIC="${2:-test-topic}"
GROUP="${3:-test-group}"

echo "Configuration:"
echo "  Broker: $BROKER"
echo "  Topic:  $TOPIC"
echo "  Group:  $GROUP"
echo ""

PS3="Select example to run: "
options=(
    "Interactive Producer"
    "Interactive Consumer"
    "Stress Test (1000 msgs)"
    "Stress Test (10000 msgs)"
    "Multi-broker Producer (broker-1)"
    "Multi-broker Consumer (broker-2)"
    "Quit"
)

select opt in "${options[@]}"
do
    case $opt in
        "Interactive Producer")
            echo ""
            echo "Starting Interactive Producer..."
            python3 examples/interactive_producer.py "$BROKER" "$TOPIC"
            break
            ;;
        "Interactive Consumer")
            echo ""
            echo "Starting Interactive Consumer..."
            python3 examples/interactive_consumer.py "$BROKER" "$TOPIC" "$GROUP"
            break
            ;;
        "Stress Test (1000 msgs)")
            echo ""
            echo "Running stress test with 1000 messages..."
            python3 examples/stress_test.py --broker "$BROKER" --topic "$TOPIC" --messages 1000 --size 100
            break
            ;;
        "Stress Test (10000 msgs)")
            echo ""
            echo "Running stress test with 10000 messages..."
            python3 examples/stress_test.py --broker "$BROKER" --topic "$TOPIC" --messages 10000 --size 100
            break
            ;;
        "Multi-broker Producer (broker-1)")
            echo ""
            echo "Starting Producer on broker-1 (localhost:9092)..."
            python3 examples/interactive_producer.py localhost:9092 cluster-test
            break
            ;;
        "Multi-broker Consumer (broker-2)")
            echo ""
            echo "Starting Consumer on broker-2 (localhost:9093)..."
            python3 examples/interactive_consumer.py localhost:9093 cluster-test cluster-group
            break
            ;;
        "Quit")
            echo "Goodbye!"
            break
            ;;
        *) 
            echo "Invalid option $REPLY"
            ;;
    esac
done
