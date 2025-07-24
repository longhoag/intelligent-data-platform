#!/usr/bin/env python3
"""
Intelligent Data Platform - Demo Runner
Simple script to run any of the available platform demos

Usage:
    python run_demo.py                    # Interactive menu
    python run_demo.py integration        # Run integration demo
    python run_demo.py day5               # Run Day 5 quality demo
    python run_demo.py all                # Run all demos
"""

import sys
import asyncio
from loguru import logger


async def run_integration_demo():
    """Run the complete integration demo"""
    logger.info("🚀 Starting Complete Integration Demo")
    from run_integration_demo import main as integration_main
    await integration_main()


async def run_day5_demo():
    """Run the Day 5 quality system demo"""
    logger.info("🔍 Starting Day 5 Quality System Demo")
    from run_day5_demo import main as day5_main
    await day5_main()


async def run_all_demos():
    """Run all available demos in sequence"""
    logger.info("🎯 Running All Platform Demos")
    
    logger.info("=" * 60)
    logger.info("📊 DEMO 1: Day 5 Data Quality & Monitoring System")
    logger.info("=" * 60)
    await run_day5_demo()
    
    logger.info("\n" + "=" * 60)
    logger.info("🏗️ DEMO 2: Complete Platform Integration")
    logger.info("=" * 60)
    await run_integration_demo()
    
    logger.success("🎉 All demos completed successfully!")


def show_menu():
    """Show interactive demo menu"""
    print("\n" + "=" * 60)
    print("🏗️  INTELLIGENT DATA PLATFORM - DEMO RUNNER")
    print("=" * 60)
    print("Choose a demo to run:")
    print()
    print("1. 🔍 Day 5: Data Quality & Monitoring System")
    print("   - Comprehensive data validation framework")
    print("   - Statistical drift detection (6 methods)")
    print("   - Real-time quality monitoring")
    print("   - Automated incident response")
    print()
    print("2. 🏗️ Complete Platform Integration Demo")
    print("   - End-to-end testing of all 5 days")
    print("   - Component health checks")
    print("   - Performance metrics")
    print("   - Comprehensive reporting")
    print()
    print("3. 🎯 All Demos (Day 5 + Integration)")
    print("   - Complete showcase of platform capabilities")
    print()
    print("0. Exit")
    print("-" * 60)
    
    while True:
        try:
            choice = input("Enter your choice (0-3): ").strip()
            if choice in ['0', '1', '2', '3']:
                return choice
            else:
                print("❌ Invalid choice. Please enter 0, 1, 2, or 3.")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            sys.exit(0)


async def main():
    """Main demo runner function"""
    # Check command line arguments
    if len(sys.argv) > 1:
        demo_type = sys.argv[1].lower()
        
        if demo_type in ['integration', 'int']:
            await run_integration_demo()
        elif demo_type in ['day5', 'quality']:
            await run_day5_demo()
        elif demo_type in ['all', 'complete']:
            await run_all_demos()
        else:
            logger.error(f"❌ Unknown demo type: {demo_type}")
            logger.info("Available options: integration, day5, all")
            sys.exit(1)
    else:
        # Interactive menu
        choice = show_menu()
        
        if choice == '0':
            logger.info("👋 Goodbye!")
            return
        elif choice == '1':
            await run_day5_demo()
        elif choice == '2':
            await run_integration_demo()
        elif choice == '3':
            await run_all_demos()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Demo interrupted by user")
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        sys.exit(1)
