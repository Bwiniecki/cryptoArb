// ===================================================================================
//  PROGRAM: janus_arbitrage_bot (main.rs)
//  VERSION: 9.2 (MEXC Zero-Fee Implementation - Auto-Rebalancing Enhanced)
//  DATE:    2025-07-03
//  PURPOSE: CEX-DEX arbitrage bot with MEXC zero-fee trading and Jupiter API
//  IMPROVEMENT: Auto-rebalancing at startup and shutdown
// ===================================================================================

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::io::AsyncBufReadExt;
use anyhow::Result;
use rust_decimal::Decimal;  
use rust_decimal::prelude::*;
use rust_decimal_macros::dec; 
use solana_sdk::signature::Signature;
use crate::solana_trading::SolanaTradingClient;
use std::collections::HashSet;
use tokio::time::sleep;

// Import modules defined below in this file
use crate::state::SharedState;

// COORDINATED TRADING AND REBALANCING SYSTEM
// Prevents conflicts between trading and rebalancing operations


// ===== COORDINATION STRUCTURES =====

#[derive(Debug, Clone)]
pub struct TradingCoordinator {
    pub is_rebalancing: Arc<AtomicBool>,
    pub active_trades: Arc<Mutex<HashSet<String>>>,
    pub active_trade_count: Arc<AtomicU32>,
}

impl TradingCoordinator {
    pub fn new() -> Self {
        Self {
            is_rebalancing: Arc::new(AtomicBool::new(false)),
            active_trades: Arc::new(Mutex::new(HashSet::new())),
            active_trade_count: Arc::new(AtomicU32::new(0)),
        }
    }

    // Check if trading should be paused
    pub fn can_start_trade(&self) -> bool {
        !self.is_rebalancing.load(Ordering::SeqCst)
    }

    // Register a new active trade
    pub fn register_trade(&self, trade_id: String) -> bool {
        if self.is_rebalancing.load(Ordering::SeqCst) {
            log::warn!("[COORD] ⚠️ Cannot start trade {} - rebalancing in progress", trade_id);
            return false;
        }

        {
            let mut trades = self.active_trades.lock().unwrap();
            trades.insert(trade_id.clone());
        }
        
        let count = self.active_trade_count.fetch_add(1, Ordering::SeqCst) + 1;
        log::debug!("[COORD] 📈 Trade {} registered. Active trades: {}", trade_id, count);
        true
    }

    // Unregister a completed trade
    pub fn unregister_trade(&self, trade_id: &str) {
        {
            let mut trades = self.active_trades.lock().unwrap();
            trades.remove(trade_id);
        }
        
        let count = self.active_trade_count.fetch_sub(1, Ordering::SeqCst) - 1;
        log::debug!("[COORD] 📉 Trade {} completed. Active trades: {}", trade_id, count);
    }

    // Get current active trade count
    pub fn get_active_count(&self) -> u32 {
        self.active_trade_count.load(Ordering::SeqCst)
    }

    // Start rebalancing (waits for trades to complete)
    pub async fn start_rebalancing(&self, timeout_seconds: u64) -> bool {
        log::info!("[COORD] 🔄 Requesting rebalancing - checking for active trades...");
        
        // Set rebalancing flag to prevent new trades
        self.is_rebalancing.store(true, Ordering::SeqCst);
        
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_seconds);
        
        // Wait for active trades to complete
        while start.elapsed() < timeout {
            let active_count = self.get_active_count();
            
            if active_count == 0 {
                log::info!("[COORD] ✅ All trades completed - rebalancing can proceed");
                return true;
            }
            
            log::info!("[COORD] ⏳ Waiting for {} active trades to complete...", active_count);
            sleep(Duration::from_secs(1)).await;
        }
        
        // Timeout reached - cancel rebalancing
        log::error!("[COORD] ⏰ Timeout waiting for trades to complete - cancelling rebalancing");
        self.is_rebalancing.store(false, Ordering::SeqCst);
        false
    }

    // Finish rebalancing
    pub fn finish_rebalancing(&self) {
        self.is_rebalancing.store(false, Ordering::SeqCst);
        log::info!("[COORD] ✅ Rebalancing completed - trading resumed");
    }

    // Emergency stop (for shutdown)
    pub fn emergency_stop(&self) {
        self.is_rebalancing.store(true, Ordering::SeqCst);
        log::warn!("[COORD] 🚨 Emergency stop - all trading paused");
    }
}

// 🚀 UPDATED main() function with startup and shutdown rebalancing

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger
    logger::init();

    log::info!("[MAIN] 🚀 MEXC Zero-Fee Implementation - Coordinated Trading Ready!");
    log::info!("[MAIN] 📊 Step 1: Fetching live account balances...");

    // 🆕 NEW: Fetch live balances FIRST before everything else
    let live_balances = match balance_fetcher::BalanceFetcher::fetch_all_balances().await {
        Ok(balances) => {
            log::info!("[MAIN] ✅ Live balances loaded successfully!");
            balances
        },
        Err(e) => {
            log::error!("[MAIN] ❌ Failed to fetch live balances: {}", e);
            log::error!("[MAIN] Cannot proceed without knowing account balances.");
            return Err(anyhow::anyhow!("Failed to fetch live balances: {}", e));
        }
    };

    // 🆕 NEW: Calculate total capital with current SOL price
    log::info!("[MAIN] 💰 Getting current SOL price for capital calculation...");
    let current_sol_price = match get_initial_sol_price().await {
        Ok(price) => {
            log::info!("[MAIN] 📈 Current SOL price: ${:.4}", price);
            price
        },
        Err(e) => {
            log::warn!("[MAIN] ⚠️ Could not fetch SOL price, using default $150: {}", e);
            dec!(150.0) // Fallback price
        }
    };

    let total_capital = live_balances.total_capital_usd(current_sol_price);
    log::info!("[MAIN] 💎 Total Capital: ${:.4} (MEXC: ${:.4} + {:.6} SOL, Wallet: ${:.4} + {:.6} SOL)", 
        total_capital, 
        live_balances.mexc_usdc, live_balances.mexc_sol,      
        live_balances.wallet_usdc, live_balances.wallet_sol
    );

    // ✅ NEW: Create trading coordinator
    let coordinator = TradingCoordinator::new();
    log::info!("[MAIN] 🤝 Trading coordinator initialized");

    // 🆕 NEW: Coordinated startup rebalancing
    log::info!("[MAIN] ⚖️ Step 2: Performing coordinated startup rebalancing...");
    if let Err(e) = perform_coordinated_startup_rebalancing(&live_balances, current_sol_price, &coordinator).await {
        log::warn!("[MAIN] ⚠️ Startup rebalancing failed: {}", e);
        log::warn!("[MAIN] Continuing with existing balances...");
    } else {
        log::info!("[MAIN] ✅ Coordinated startup rebalancing completed!");
        
        // Fetch updated balances after rebalancing
        if let Ok(updated_balances) = balance_fetcher::BalanceFetcher::fetch_all_balances().await {
            log::info!("[MAIN] 📊 Updated balances after startup rebalancing:");
            log::info!("[MAIN] 💰 MEXC: ${:.4} USDC + {:.6} SOL", updated_balances.mexc_usdc, updated_balances.mexc_sol);
            log::info!("[MAIN] 🏦 Wallet: ${:.4} USDC + {:.6} SOL", updated_balances.wallet_usdc, updated_balances.wallet_sol);
        }
    }

    // Re-fetch balances for trading (in case rebalancing changed them)
    let final_live_balances = balance_fetcher::BalanceFetcher::fetch_all_balances().await
        .unwrap_or(live_balances); // Fallback to original if fetch fails

    // 🆕 NEW: Risk check before starting
    let max_trade_value = total_capital * (config::MAX_CAPITAL_PER_TRADE_PERCENT / dec!(100.0));
    let current_trade_value = config::TRADE_AMOUNT_SOL * current_sol_price;
    
    log::info!("[MAIN] 🛡️  Risk Check: Trade size ${:.2} vs Max allowed ${:.2}", 
        current_trade_value, max_trade_value);
    
    if current_trade_value > max_trade_value {
        log::error!("[MAIN] ❌ RISK VIOLATION: Trade size exceeds maximum allowed!");
        log::error!("[MAIN] Reduce TRADE_AMOUNT_SOL or increase capital.");
        return Err(anyhow::anyhow!("Trade size exceeds risk limits"));
    }

    // The application's run state
    let is_running = Arc::new(AtomicBool::new(true));

    // 🔧 UPDATED: Initialize shared state with LIVE balances (after rebalancing)
    let shared_state = Arc::new(Mutex::new(SharedState::new_with_live_balances(
        final_live_balances.mexc_usdc,
        final_live_balances.mexc_sol,        
        final_live_balances.wallet_usdc,
        final_live_balances.wallet_sol,      
        total_capital
    )));

    // Create a channel for price updates
    let (price_sender, price_receiver) = mpsc::channel(128);

    // Clone Arcs for each task
    let engine_state = Arc::clone(&shared_state);
    let final_state_clone = Arc::clone(&shared_state);
    let coordinator_engine = coordinator.clone();
    let coordinator_rebalance = coordinator.clone();
    let coordinator_shutdown = coordinator.clone();

    let mexc_running = Arc::clone(&is_running);
    let solana_running = Arc::clone(&is_running);
    let engine_running = Arc::clone(&is_running);
    let failsafe_running = Arc::clone(&is_running);

    log::info!("[MAIN] 🚀 Starting coordinated trading threads...");

    // Task for MEXC WebSocket connector
    let mexc_sender = price_sender.clone();
    let mexc_handle = tokio::spawn(async move {
        mexc::run_mexc_connector(mexc_running, mexc_sender).await;
    });

    // Task for Solana Jupiter API connector
    let solana_handle = tokio::spawn(async move {
        solana::run_jupiter_connector(solana_running, price_sender).await;
    });

    // Give connectors time to get initial prices
    log::info!("[MAIN] ⏳ Waiting for initial price feeds...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ✅ UPDATED: Coordinated arbitrage engine
    let engine_handle = tokio::spawn(async move {
        engine::run_coordinated_arbitrage_engine(engine_running, engine_state, price_receiver, coordinator_engine).await;
    });

    // ✅ UPDATED: Coordinated auto-rebalancing
    let rebalance_state = Arc::clone(&shared_state);
    let rebalance_running = Arc::clone(&is_running);
    let rebalance_handle = tokio::spawn(async move {
        auto_rebalancer::run_coordinated_rebalance_monitor(rebalance_running, rebalance_state, coordinator_rebalance).await;
    });

    // Enhanced failsafe with coordination
    let failsafe_handle = tokio::spawn(async move {
        log::info!(">> Enhanced Coordinated Failsafe armed. Press [ENTER] for coordinated shutdown. <<");
        let _ = tokio::io::BufReader::new(tokio::io::stdin()).lines().next_line().await;
        
        if failsafe_running.load(Ordering::SeqCst) {
            log::warn!("[FAILSAFE] Manual KILL SWITCH engaged - initiating coordinated shutdown.");
            
            // Emergency stop trading to prevent new trades
            coordinator_shutdown.emergency_stop();
            failsafe_running.store(false, Ordering::SeqCst);
        }
    });

    tokio::select! {
        _ = mexc_handle => log::error!("[MAIN] MEXC connector exited unexpectedly."),
        _ = solana_handle => log::error!("[MAIN] Solana connector exited unexpectedly."),
        _ = engine_handle => log::info!("[MAIN] Coordinated arbitrage engine shut down."),
        _ = rebalance_handle => log::info!("[MAIN] Coordinated rebalance monitor shut down."),
        _ = failsafe_handle => log::info!("[MAIN] Coordinated failsafe shutdown triggered."),
    }

    // Ensure all tasks shut down
    is_running.store(false, Ordering::SeqCst);
    coordinator.emergency_stop();

    // 🆕 NEW: Coordinated shutdown rebalancing
    log::info!("[MAIN] ⚖️ Performing coordinated shutdown rebalancing...");
    if let Err(e) = perform_coordinated_shutdown_rebalancing(current_sol_price, &coordinator).await {
        log::warn!("[MAIN] ⚠️ Shutdown rebalancing failed: {}", e);
    } else {
        log::info!("[MAIN] ✅ Coordinated shutdown rebalancing completed!");
    }

    // 🆕 NEW: Show final balance comparison with TRADING-BASED P&L
    log::info!("[MAIN] 💰 Fetching final balances for P&L calculation...");
    if let Ok(final_balances) = balance_fetcher::BalanceFetcher::fetch_all_balances().await {
        let final_capital = final_balances.total_capital_usd(current_sol_price);
        let capital_change = final_capital - total_capital;
        
        // Get trading statistics
        let trading_stats = {
            let final_state_lock = final_state_clone.lock().unwrap();
            final_state_lock.get_trading_stats()
        };
        
        log::info!("[MAIN] 📊 FINAL SESSION SUMMARY:");
        log::info!("[MAIN] 💰 Starting Capital: ${:.4}", total_capital);
        log::info!("[MAIN] 💰 Final Capital: ${:.4}", final_capital);
        log::info!("[MAIN] 📈 Capital Change: ${:+.4} ({:+.2}%) - includes market movements", 
            capital_change, (capital_change / total_capital * dec!(100.0)));
        
        log::info!("[MAIN] 🎯 TRADING PERFORMANCE:");
        log::info!("[MAIN] 📈 Trading P&L: ${:+.4} ({:+.2}%) - pure trading results", 
            trading_stats.trading_pnl, trading_stats.trading_pnl_percent);
        log::info!("[MAIN] 📊 Total Trades: {} (Success: {:.1}%)", 
            trading_stats.total_trades, trading_stats.success_rate);
        log::info!("[MAIN] 💸 Total Fees Paid: ${:.4}", trading_stats.total_fees_paid);
        
        if trading_stats.total_trades > 0 {
            log::info!("[MAIN] 🏆 Best Trade: ${:+.4} | Worst Trade: ${:+.4}", 
                trading_stats.largest_win, trading_stats.largest_loss);
            log::info!("[MAIN] 📊 Avg Profit per Trade: ${:+.4}", 
                trading_stats.avg_profit_per_trade);
        }
        
        // 🆕 NEW: Show final balance breakdown
        log::info!("[MAIN] 📊 FINAL BALANCE BREAKDOWN:");
        log::info!("[MAIN] 💰 MEXC: ${:.4} USDC + {:.6} SOL", final_balances.mexc_usdc, final_balances.mexc_sol);
        log::info!("[MAIN] 🏦 Wallet: ${:.4} USDC + {:.6} SOL", final_balances.wallet_usdc, final_balances.wallet_sol);
        
        // 🆕 NEW: Risk analysis summary
        if trading_stats.trading_pnl_percent < Decimal::ZERO {
            let drawdown_percent = trading_stats.trading_pnl_percent.abs();
            if drawdown_percent >= config::MAX_DAILY_DRAWDOWN_PERCENT {
                log::warn!("[MAIN] ⚠️ RISK: Trading drawdown of {:.2}% reached daily limit of {:.2}%", 
                    drawdown_percent, config::MAX_DAILY_DRAWDOWN_PERCENT);
            } else {
                log::info!("[MAIN] 🛡️ RISK: Trading drawdown of {:.2}% within limit of {:.2}%", 
                    drawdown_percent, config::MAX_DAILY_DRAWDOWN_PERCENT);
            }
        } else {
            log::info!("[MAIN] 🎉 RISK: Positive trading P&L - no drawdown concerns");
        }
        
        // Trading duration
        log::info!("[MAIN] ⏱️ Trading Duration: {}m {}s", 
            trading_stats.trading_duration.num_minutes(), 
            trading_stats.trading_duration.num_seconds() % 60);
    }
    
    // Write price and trade history to CSV (existing code)
    log::info!("[MAIN] 📄 Writing trading history to CSV...");
    let final_state_lock = final_state_clone.lock();
    match final_state_lock {
        Ok(final_state) => {
            // Write price history
            if let Err(e) = csv_writer::write_to_csv(&final_state.price_history) {
                log::error!("[MAIN] Failed to write price CSV: {}", e);
            }
            
            // Write trade history
            if let Err(e) = trade_csv_writer::write_trades_to_csv(&final_state.trade_history) {
                log::error!("[MAIN] Failed to write trades CSV: {}", e);
            } else {
                log::info!("[MAIN] ✅ Successfully wrote trade history to CSV.");
            }
            
            // ✅ FIX: Get final balances and trading stats in this scope
            match balance_fetcher::BalanceFetcher::fetch_all_balances().await {
                Ok(final_balances) => {
                    // Get trading statistics from the final state
                    let trading_stats = final_state.get_trading_stats();
                    
                    // ✅ ENHANCED: Write comprehensive trading summary with balances
                    let session_duration = trading_stats.trading_duration.num_minutes();
                    if let Err(e) = trade_csv_writer::write_enhanced_trade_summary(
                        &final_state.trade_history, 
                        final_state.total_profit, 
                        final_state.trade_count,
                        &final_live_balances,  // Starting balances
                        &final_balances,       // Ending balances  
                        current_sol_price,     // SOL price for calculations
                        session_duration       // Session duration in minutes
                    ) {
                        log::error!("[MAIN] Failed to write enhanced summary: {}", e);
                    } else {
                        log::info!("[MAIN] ✅ Successfully wrote enhanced trading summary with balance details.");
                    }
                },
                Err(e) => {
                    log::error!("[MAIN] Failed to fetch final balances for enhanced summary: {}", e);
                    
                    // Fallback to basic summary
                    if let Err(e) = trade_csv_writer::write_trade_summary(
                        &final_state.trade_history, 
                        final_state.total_profit, 
                        final_state.trade_count
                    ) {
                        log::error!("[MAIN] Failed to write basic summary: {}", e);
                    } else {
                        log::info!("[MAIN] ✅ Successfully wrote basic trading summary.");
                    }
                }
            }
            
            log::info!("[MAIN] 📊 SESSION STATS: {} trades, ${:.4} total profit", 
                final_state.trade_count, final_state.total_profit);
        },
        Err(poison_error) => {
            log::error!("[MAIN] Mutex was poisoned, cannot write CSV. Error: {}", poison_error);
        }
    }

    log::info!("[MAIN] 🏁 All threads terminated. Coordinated shutdown complete.");
    Ok(())
}

// ✅ ENHANCED: Add balance verification after rebalancing
async fn verify_rebalancing_success(
    _solana_client: &SolanaTradingClient,
    initial_balances: &balance_fetcher::LiveBalances,
    sol_price: Decimal,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    log::info!("[REBALANCE] 🔍 Verifying rebalancing results...");
    
    // Wait for balances to settle
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Fetch new balances
    let new_balances = balance_fetcher::BalanceFetcher::fetch_all_balances().await?;
    
    // Compare balances
    let initial_total = initial_balances.total_capital_usd(sol_price);
    let final_total = new_balances.total_capital_usd(sol_price);
    let capital_change = final_total - initial_total;
    
    log::info!("[REBALANCE] 📊 Rebalancing Results:");
    log::info!("[REBALANCE] 💰 Initial Capital: ${:.4}", initial_total);
    log::info!("[REBALANCE] 💰 Final Capital: ${:.4}", final_total);
    log::info!("[REBALANCE] 📈 Capital Change: ${:+.4} ({:+.2}%)", 
        capital_change, (capital_change / initial_total * dec!(100.0)));
    
    // Check if rebalancing was successful
    let wallet_usdc_value = new_balances.wallet_usdc;
    let wallet_sol_value = new_balances.wallet_sol * sol_price;
    let wallet_total = wallet_usdc_value + wallet_sol_value;
    
    if wallet_total > Decimal::ZERO {
        let wallet_usdc_percent = (wallet_usdc_value / wallet_total) * dec!(100.0);
        let wallet_sol_percent = (wallet_sol_value / wallet_total) * dec!(100.0);
        
        log::info!("[REBALANCE] 📊 Final Wallet Split: {:.1}% USDC / {:.1}% SOL", 
            wallet_usdc_percent, wallet_sol_percent);
        
        // Check if we're now within the acceptable range
        let target_percent = config::REBALANCE_TARGET_PERCENT;
        let deadband = config::REBALANCE_DEADBAND_PERCENT;
        let min_acceptable = target_percent - deadband;
        let max_acceptable = target_percent + deadband;
        
        if wallet_usdc_percent >= min_acceptable && wallet_usdc_percent <= max_acceptable {
            log::info!("[REBALANCE] ✅ Rebalancing successful - within target range");
        } else {
            log::warn!("[REBALANCE] ⚠️ Rebalancing incomplete - still outside target range");
        }
    }
    
    Ok(())
}
// 🆕 NEW: Wallet startup rebalancing
async fn rebalance_wallet_startup(
    solana_client: &solana_trading::SolanaTradingClient,
    balances: &balance_fetcher::LiveBalances,
    sol_price: Decimal
) -> Result<()> {
    let wallet_usdc_value = balances.wallet_usdc;
    let wallet_sol_value = balances.wallet_sol * sol_price;
    let wallet_total = wallet_usdc_value + wallet_sol_value;
    
    if wallet_total < config::REBALANCE_MIN_VALUE_USD {
        log::info!("[STARTUP_REBALANCE] ⏭️ Wallet total ${:.2} below minimum, skipping", wallet_total);
        return Ok(());
    }
    
    let wallet_usdc_percent = (wallet_usdc_value / wallet_total) * dec!(100.0);
    let wallet_sol_percent = (wallet_sol_value / wallet_total) * dec!(100.0);
    
    log::info!("[STARTUP_REBALANCE] 📊 Wallet Current: {:.1}% USDC / {:.1}% SOL", wallet_usdc_percent, wallet_sol_percent);
    
    if wallet_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT {
        // Too much USDC, buy SOL
        let target_usdc_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
        let usdc_to_convert = wallet_usdc_value - target_usdc_value;
        
        if usdc_to_convert >= config::REBALANCE_MIN_TRADE_USD {
            let usdc_lamports = (usdc_to_convert * dec!(1_000_000)).to_u64().unwrap_or(0);
            
            log::info!("[STARTUP_REBALANCE] 🔄 Wallet: Converting ${:.2} USDC to SOL", usdc_to_convert);
            
            // ✅ ENHANCED: Use confirmed swap with verification
            match solana_client.execute_swap_with_confirmation(
                config::USDC_MINT_ADDRESS,
                config::SOL_MINT_ADDRESS,
                usdc_lamports,
                50, // ✅ REDUCED: 0.5% slippage instead of 1%
            ).await {
                Ok(result) => {
                    if result.confirmed {
                        log::info!("[STARTUP_REBALANCE] ✅ Wallet rebalancing completed in {}ms", result.confirmation_time_ms);
                        log::info!("[STARTUP_REBALANCE] 📊 Final balances: ${:.4} USDC, {:.6} SOL", 
                            result.final_balance_usdc.unwrap_or(Decimal::ZERO), 
                            result.final_balance_sol.unwrap_or(Decimal::ZERO));
                    } else {
                        log::error!("[STARTUP_REBALANCE] ❌ Wallet rebalancing failed - transaction not confirmed");
                        return Err(anyhow::anyhow!("Wallet rebalancing transaction not confirmed"));
                    }
                },
                Err(e) => {
                    log::error!("[STARTUP_REBALANCE] ❌ Wallet rebalancing failed: {}", e);
                    return Err(anyhow::anyhow!("Solana USDC->SOL swap failed: {}", e));
                }
            }
        }
    } else if wallet_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
        // Too much SOL, sell SOL
        let target_sol_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
        let sol_value_to_convert = wallet_sol_value - target_sol_value;
        
        if sol_value_to_convert >= config::REBALANCE_MIN_TRADE_USD {
            let sol_to_convert = sol_value_to_convert / sol_price;
            let sol_lamports = (sol_to_convert * dec!(1_000_000_000)).to_u64().unwrap_or(0);
            
            log::info!("[STARTUP_REBALANCE] 🔄 Wallet: Converting {:.4} SOL to USDC", sol_to_convert);
            
            // ✅ ENHANCED: Use confirmed swap with verification
            match solana_client.execute_swap_with_confirmation(
                config::SOL_MINT_ADDRESS,
                config::USDC_MINT_ADDRESS,
                sol_lamports,
                50, // ✅ REDUCED: 0.5% slippage instead of 1%
            ).await {
                Ok(result) => {
                    if result.confirmed {
                        log::info!("[STARTUP_REBALANCE] ✅ Wallet rebalancing completed in {}ms", result.confirmation_time_ms);
                        log::info!("[STARTUP_REBALANCE] 📊 Final balances: ${:.4} USDC, {:.6} SOL", 
                            result.final_balance_usdc.unwrap_or(Decimal::ZERO), 
                            result.final_balance_sol.unwrap_or(Decimal::ZERO));
                    } else {
                        log::error!("[STARTUP_REBALANCE] ❌ Wallet rebalancing failed - transaction not confirmed");
                        return Err(anyhow::anyhow!("Wallet rebalancing transaction not confirmed"));
                    }
                },
                Err(e) => {
                    log::error!("[STARTUP_REBALANCE] ❌ Wallet rebalancing failed: {}", e);
                    return Err(anyhow::anyhow!("Solana SOL->USDC swap failed: {}", e));
                }
            }
        }
    } else {
        log::info!("[STARTUP_REBALANCE] ✅ Wallet already balanced");
    }
    
    Ok(())
}

// 🆕 NEW: MEXC startup rebalancing
async fn rebalance_mexc_startup(
    mexc_client: &mexc_trading::MexcTradingClient,
    balances: &balance_fetcher::LiveBalances,
    sol_price: Decimal
) -> Result<()> {
    let mexc_usdc_value = balances.mexc_usdc;
    let mexc_sol_value = balances.mexc_sol * sol_price;
    let mexc_total = mexc_usdc_value + mexc_sol_value;
    
    if mexc_total < config::REBALANCE_MIN_VALUE_USD {
        log::info!("[STARTUP_REBALANCE] ⏭️ MEXC total ${:.2} below minimum, skipping", mexc_total);
        return Ok(());
    }
    
    let mexc_usdc_percent = (mexc_usdc_value / mexc_total) * dec!(100.0);
    let mexc_sol_percent = (mexc_sol_value / mexc_total) * dec!(100.0);
    
    log::info!("[STARTUP_REBALANCE] 📊 MEXC Current: {:.1}% USDC / {:.1}% SOL", mexc_usdc_percent, mexc_sol_percent);
    
    if mexc_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT {
        // Too much USDC, buy SOL
        let target_usdc_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
        let usdc_to_convert = mexc_usdc_value - target_usdc_value;
        
        if usdc_to_convert >= config::REBALANCE_MIN_TRADE_USD {
            let sol_to_buy = usdc_to_convert / sol_price;
            
            log::info!("[STARTUP_REBALANCE] 🔄 MEXC: Converting ${:.2} USDC to {:.4} SOL", usdc_to_convert, sol_to_buy);
            
            mexc_client.place_order_with_current_price(
                "SOLUSDC", "BUY", "MARKET", sol_to_buy, None
            ).await.map_err(|e| anyhow::anyhow!("MEXC buy order failed: {}", e))?;
            
            log::info!("[STARTUP_REBALANCE] ✅ MEXC rebalancing completed");
        }
    } else if mexc_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
        // Too much SOL, sell SOL
        let target_sol_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
        let sol_value_to_convert = mexc_sol_value - target_sol_value;
        
        if sol_value_to_convert >= config::REBALANCE_MIN_TRADE_USD {
            let sol_to_sell = sol_value_to_convert / sol_price;
            
            log::info!("[STARTUP_REBALANCE] 🔄 MEXC: Converting {:.4} SOL to ${:.2} USDC", sol_to_sell, sol_value_to_convert);
            
            mexc_client.place_order_with_current_price(
                "SOLUSDC", "SELL", "MARKET", sol_to_sell, None
            ).await.map_err(|e| anyhow::anyhow!("MEXC sell order failed: {}", e))?;
            
            log::info!("[STARTUP_REBALANCE] ✅ MEXC rebalancing completed");
        }
    } else {
        log::info!("[STARTUP_REBALANCE] ✅ MEXC already balanced");
    }
    
    Ok(())
}
// 🆕 NEW: Startup rebalancing function
async fn perform_startup_rebalancing(
    balances: &balance_fetcher::LiveBalances, 
    sol_price: Decimal
) -> Result<()> {
    log::info!("[STARTUP_REBALANCE] 🔄 Analyzing account balances for optimal trading setup...");
    
    // Initialize trading clients
    let app_config = config::AppConfig::new();
    
    let mexc_client = mexc_trading::MexcTradingClient::new(
        app_config.mexc_api_key.clone(),
        app_config.mexc_api_secret.clone()
    );
    
    let solana_client = solana_trading::SolanaTradingClient::new(
        app_config.solana_rpc_url.clone(),
        app_config.solana_wallet_path.clone(),
        app_config.jupiter_api_key.clone()
    ).map_err(|e| anyhow::anyhow!("Failed to create Solana client: {}", e))?;
    
    // Check and rebalance MEXC
    rebalance_mexc_startup(&mexc_client, balances, sol_price).await?;
    
    // Check and rebalance Solana wallet
    rebalance_wallet_startup(&solana_client, balances, sol_price).await?;
    
    Ok(())
}
// 🆕 NEW: Shutdown rebalancing function
async fn perform_shutdown_rebalancing(sol_price: Decimal) -> Result<()> {
    log::info!("[SHUTDOWN_REBALANCE] 🔄 Rebalancing accounts for optimal storage...");
    
    // Fetch current balances
    let current_balances = balance_fetcher::BalanceFetcher::fetch_all_balances().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch balances: {}", e))?;
    
    // Initialize trading clients
    let app_config = config::AppConfig::new();
    
    let mexc_client = mexc_trading::MexcTradingClient::new(
        app_config.mexc_api_key.clone(),
        app_config.mexc_api_secret.clone()
    );
    
    let solana_client = solana_trading::SolanaTradingClient::new(
        app_config.solana_rpc_url.clone(),
        app_config.solana_wallet_path.clone(),
        app_config.jupiter_api_key.clone()
    ).map_err(|e| anyhow::anyhow!("Failed to create Solana client: {}", e))?;
    
    // Rebalance both accounts to 50/50
    rebalance_mexc_shutdown(&mexc_client, &current_balances, sol_price).await?;
    rebalance_wallet_shutdown(&solana_client, &current_balances, sol_price).await?;
    
    Ok(())
}
async fn perform_coordinated_shutdown_rebalancing(
    sol_price: Decimal,
    coordinator: &TradingCoordinator,
) -> Result<()> {
    log::info!("[SHUTDOWN_REBALANCE] 🔄 Starting coordinated shutdown rebalancing...");
    
    // Request rebalancing (this will prevent new trades)
    if !coordinator.start_rebalancing(60).await { // Longer timeout for shutdown
        log::warn!("[SHUTDOWN_REBALANCE] ⚠️ Some trades did not complete - proceeding anyway");
    }
    
    // Perform rebalancing
    let result = perform_shutdown_rebalancing(sol_price).await;
    
    // Note: Don't resume trading since we're shutting down
    
    result
}
// ===== COORDINATED STARTUP REBALANCING =====

async fn perform_coordinated_startup_rebalancing(
    balances: &balance_fetcher::LiveBalances, 
    sol_price: Decimal,
    coordinator: &TradingCoordinator,
) -> Result<()> {
    log::info!("[STARTUP_REBALANCE] 🔄 Starting coordinated rebalancing...");
    
    // Request rebalancing (this will prevent new trades)
    if !coordinator.start_rebalancing(30).await {
        return Err(anyhow::anyhow!("Failed to start rebalancing - active trades did not complete"));
    }
    
    // Perform rebalancing
    let result = perform_startup_rebalancing(balances, sol_price).await;
    
    // Always finish rebalancing to resume trading
    coordinator.finish_rebalancing();
    
    result
}
// 🆕 NEW: MEXC shutdown rebalancing (same logic as startup)
async fn rebalance_mexc_shutdown(
    mexc_client: &mexc_trading::MexcTradingClient,
    balances: &balance_fetcher::LiveBalances,
    sol_price: Decimal
) -> Result<()> {
    rebalance_mexc_startup(mexc_client, balances, sol_price).await
}


// 🆕 NEW: Wallet shutdown rebalancing (same logic as startup)
async fn rebalance_wallet_shutdown(
    solana_client: &solana_trading::SolanaTradingClient,
    balances: &balance_fetcher::LiveBalances,
    sol_price: Decimal
) -> Result<()> {
    rebalance_wallet_startup(solana_client, balances, sol_price).await
}
// ---- ENHANCED: Updated get_initial_sol_price with better headers ----
// 🆕 NEW: Helper function to get initial SOL price with Jupiter Pro (ENHANCED)
async fn get_initial_sol_price() -> Result<Decimal> {
    let app_config = config::AppConfig::new();
    
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("x-api-key", reqwest::header::HeaderValue::from_str(&app_config.jupiter_api_key).unwrap());
            headers
        })
        .build()?;
    
    let url = format!("{}?ids={}", app_config.jupiter_api_url, config::SOL_MINT_ADDRESS);
    
    let response = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        
        if status.as_u16() == 429 {
            log::error!("[MAIN] 🚨 Initial price fetch rate limited! Check Pro API key activation.");
            log::error!("[MAIN] 🚨 Response: {}", error_text);
        }
        
        return Err(anyhow::anyhow!("HTTP error ({}): {}", status, error_text));
    }

    let price_data: serde_json::Value = response.json().await?;
    
    // ✅ UPDATED: New v3 response format - direct access, no "data" wrapper
    if let Some(sol_price) = price_data[config::SOL_MINT_ADDRESS]["usdPrice"].as_f64() {
        let price_decimal = Decimal::from_f64(sol_price)
            .ok_or_else(|| anyhow::anyhow!("Failed to convert Jupiter price to Decimal"))?;
        
        log::info!("[MAIN] ✅ Initial Pro API price: ${:.4}", price_decimal);
        return Ok(price_decimal);
    }
    
    Err(anyhow::anyhow!("SOL price not found in Jupiter v3 response"))
}
// ==================================================
// MODULES
// ==================================================

// ---- FIXED Configuration for Real Trading ----
mod config {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::env;

    fn get_env_var(key: &str) -> String {
        env::var(key).unwrap_or_else(|_| panic!("ERROR: Missing required environment variable '{}'", key))
    }

    fn get_env_var_optional(key: &str, default: &str) -> String {
        env::var(key).unwrap_or_else(|_| default.to_string())
    }

    #[allow(dead_code)]
    pub struct AppConfig {
        pub mexc_api_key: String,
        pub mexc_api_secret: String,
        pub jupiter_api_url: String,
        pub jupiter_api_key: String,  // ✅ NEW: Jupiter Pro API key
        pub solana_rpc_url: String,
        pub solana_wallet_path: String,
    }

    impl AppConfig {
        pub fn new() -> Self {
            Self {
                mexc_api_key: get_env_var("MEXC_API_KEY"),
                mexc_api_secret: get_env_var("MEXC_API_SECRET"),
                jupiter_api_url: get_env_var_optional("JUPITER_API_URL", "https://api.jup.ag/price/v3"),
                jupiter_api_key: get_env_var_optional("JUPITER_API_KEY", "4281fd8b-73a8-4984-9dd8-9dba742597f2"),
                solana_rpc_url: get_env_var_optional("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
                solana_wallet_path: get_env_var("SOLANA_WALLET_PRIVATE_KEY_PATH"),
            }
        }
    }

    // 🚨 REAL TRADING MODE ENABLED!
    pub const PAPER_TRADING: bool = false;

    // Static config
    pub const MEXC_WEBSOCKET_URL: &str = "wss://wbs.mexc.com/ws";
    pub const MEXC_TARGET_PAIR: &str = "SOLUSDC";
    pub const SOL_MINT_ADDRESS: &str = "So11111111111111111111111111111111111111112";
    pub const USDC_MINT_ADDRESS: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    // ✅ DEPRECATED: These are now only used for backwards compatibility in paper trading
    // Real trading uses live balances fetched at startup
    pub const MEXC_STARTING_USDC: Decimal = dec!(996.78905698);    // ⚠️ DEPRECATED: Only for paper trading fallback
    pub const WALLET_STARTING_USDC: Decimal = dec!(778.71896);     // ⚠️ DEPRECATED: Only for paper trading fallback

    // ✅ OPTIMIZED: Strategy & Risk - Now uses live capital calculations
    pub const MIN_PROFIT_SPREAD_PERCENT: Decimal = dec!(0.1);     // 0.02% minimum profit
    pub const TRADE_AMOUNT_SOL: Decimal = dec!(1.0);              // 1.0 SOL trades
    pub const MAX_CAPITAL_PER_TRADE_PERCENT: Decimal = dec!(50.0); // 50% max per trade (of LIVE capital)
    pub const MAX_DAILY_DRAWDOWN_PERCENT: Decimal = dec!(20.0);    // 20% max daily loss (from LIVE starting capital)
    
    // Fees and slippage
    pub const SOLANA_GAS_FEE_USD: Decimal = dec!(0.01);
    pub const MAX_EXECUTION_TIME_SECONDS: u64 = 5;
    pub const MAX_PRICE_AGE_SECONDS: u64 = 15;
    pub const SOLANA_MAX_SLIPPAGE_BPS: u16 = 15;
    
    // ✅ NEW: Auto-rebalancing configuration
    pub const AUTO_REBALANCE_ENABLED: bool = true;
    pub const REBALANCE_THRESHOLD_PERCENT: Decimal = dec!(60.0);  // Trigger when 70% or more in one asset
    pub const REBALANCE_TARGET_PERCENT: Decimal = dec!(50.0);     // Target 50/50 split
    pub const REBALANCE_CHECK_INTERVAL_SECONDS: u64 = 10;        // Check every 30 seconds
    pub const REBALANCE_MIN_VALUE_USD: Decimal = dec!(100.0);    // Only rebalance if imbalance > $100  
    pub const REBALANCE_MIN_TRADE_USD: Decimal = dec!(50.0); 
    pub const REBALANCE_CONFIRMATION_TIMEOUT_SECONDS: u64 = 30;

    // ✅ ENHANCED: Better balance target ranges
    pub const REBALANCE_DEADBAND_PERCENT: Decimal = dec!(5.0);   // ✅ NEW: 5% deadband to prevent oscillation
    // Only rebalance if outside 45-55% range (50% ± 5%)
    
    // 🚀 NEW: Jupiter Pro Rate Limiting Configuration
    pub const JUPITER_PRO_RPM_LIMIT: u32 = 3000;                    // Total Pro plan limit 600
    pub const JUPITER_PRICE_UPDATE_RPS: u32 = 2;                   // 3 RPS for price updates 2
    pub const JUPITER_TRADING_BUDGET_RPS: u32 = 45;                 // 7 RPS for trading calls 7 
    
    // Calculated intervals
    pub const JUPITER_PRICE_INTERVAL_MS: u64 = 500 / JUPITER_PRICE_UPDATE_RPS as u64; // 333ms 1000
    pub const JUPITER_TRADING_MIN_INTERVAL_MS: u64 = 25 / JUPITER_TRADING_BUDGET_RPS as u64; // 143ms 1000
    
    // 🔧 EASILY ADJUSTABLE: Change these if you need different balance
    // For more trading headroom: Reduce JUPITER_PRICE_UPDATE_RPS to 2, increase JUPITER_TRADING_BUDGET_RPS to 8
    // For more accurate prices: Increase JUPITER_PRICE_UPDATE_RPS to 4, reduce JUPITER_TRADING_BUDGET_RPS to 6
}


// ---- OPTIMIZED MEXC Trading Execution Module ----
mod mexc_trading {
    use reqwest;
    use serde_json::Value;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use rust_decimal::Decimal;
    use std::time::{SystemTime, UNIX_EPOCH};
    use rust_decimal_macros::dec;

    type HmacSha256 = Hmac<Sha256>;

    #[derive(Clone)]
    pub struct MexcTradingClient {
        client: reqwest::Client,
        api_key: String,
        api_secret: String,
        base_url: String,
    }

    impl MexcTradingClient {
        pub fn new(api_key: String, api_secret: String) -> Self {
            // Optimized client with connection pooling and aggressive timeouts
            let client = reqwest::Client::builder()
                .tcp_keepalive(std::time::Duration::from_secs(30))
                .tcp_nodelay(true) // Disable Nagle's algorithm for lower latency
                .pool_max_idle_per_host(20)
                .timeout(std::time::Duration::from_secs(3)) // Aggressive 3s timeout
                .build()
                .unwrap();
                
            Self {
                client,
                api_key,
                api_secret,
                base_url: "https://api.mexc.com".to_string(),
            }
        }

        fn generate_signature(&self, params: &str) -> String {
            let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes()).unwrap();
            mac.update(params.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        }

        // 🆕 NEW: Get orderbook for precise pricing
        pub async fn get_orderbook(
            &self,
            symbol: &str,
            limit: u32,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let url = format!("{}/api/v3/depth?symbol={}&limit={}", 
                self.base_url, symbol, limit);
            
            let response = self.client
                .get(&url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()
                .await?;

            if response.status().is_success() {
                let orderbook: Value = response.json().await?;
                Ok(orderbook)
            } else {
                let error_text = response.text().await?;
                Err(format!("Failed to get orderbook: {}", error_text).into())
            }
        }

        // 🆕 NEW: Optimized market order using orderbook pricing
        pub async fn place_market_order_optimized(
            &self,
            symbol: &str,
            side: &str,  // "BUY" or "SELL"
            quantity: Decimal,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            // Get orderbook for precise pricing
            let orderbook = self.get_orderbook(symbol, 5).await?;
            
            // Extract best bid/ask with minimal slippage
            let execution_price = if side == "BUY" {
                // For BUY orders, use best ask + tiny premium
                if let Some(asks) = orderbook.get("asks").and_then(|a| a.as_array()) {
                    if let Some(best_ask) = asks.first().and_then(|a| a.as_array()) {
                        if let Some(price_str) = best_ask.first().and_then(|p| p.as_str()) {
                            let ask_price = price_str.parse::<Decimal>()
                                .map_err(|_| "Failed to parse ask price")?;
                            ask_price * dec!(1.0001) // Only 0.01% premium
                        } else {
                            return Err("No ask price found".into());
                        }
                    } else {
                        return Err("No asks in orderbook".into());
                    }
                } else {
                    return Err("No asks array in orderbook".into());
                }
            } else {
                // For SELL orders, use best bid - tiny discount
                if let Some(bids) = orderbook.get("bids").and_then(|b| b.as_array()) {
                    if let Some(best_bid) = bids.first().and_then(|b| b.as_array()) {
                        if let Some(price_str) = best_bid.first().and_then(|p| p.as_str()) {
                            let bid_price = price_str.parse::<Decimal>()
                                .map_err(|_| "Failed to parse bid price")?;
                            bid_price * dec!(0.9999) // Only 0.01% discount
                        } else {
                            return Err("No bid price found".into());
                        }
                    } else {
                        return Err("No bids in orderbook".into());
                    }
                } else {
                    return Err("No bids array in orderbook".into());
                }
            };

            log::info!("[MEXC] 🎯 Optimized {} order: {} {} at ${:.4} (from orderbook)", 
                side, quantity, symbol, execution_price);

            // Place LIMIT order with FOK (Fill or Kill) for immediate execution
            self.place_limit_order_fok(symbol, side, quantity, execution_price).await
        }

        // 🆕 NEW: FOK order for all-or-nothing execution
        async fn place_limit_order_fok(
            &self,
            symbol: &str,
            side: &str,
            quantity: Decimal,
            price: Decimal,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis() as u64;

            let mut params = vec![
                ("symbol", symbol.to_string()),
                ("side", side.to_string()),
                ("type", "LIMIT".to_string()),
                ("quantity", quantity.to_string()),
                ("price", price.to_string()),
                ("timeInForce", "FOK".to_string()), // Fill or Kill - all or nothing
                ("timestamp", timestamp.to_string()),
            ];

            // Sort parameters for signature
            params.sort_by(|a, b| a.0.cmp(b.0));
            
            let query_string = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");

            let signature = self.generate_signature(&query_string);
            let final_url = format!("{}/api/v3/order?{}&signature={}", 
                self.base_url, query_string, signature);
            
            log::debug!("[MEXC] FOK order URL: {}", final_url);
            
            let response = self.client
                .post(&final_url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()
                .await?;

            if response.status().is_success() {
                let result: Value = response.json().await?;
                log::info!("[MEXC] ✅ FOK order executed successfully");
                Ok(result)
            } else {
                let error_text = response.text().await?;
                log::error!("[MEXC] ❌ FOK order failed: {}", error_text);
                Err(format!("MEXC FOK Order Error: {}", error_text).into())
            }
        }

        // Keep original method for backward compatibility but use optimized version
        pub async fn place_order(
            &self,
            symbol: &str,
            side: &str,
            order_type: &str,
            quantity: Decimal,
            price: Option<Decimal>,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            // If MARKET order requested, use optimized version
            if order_type == "MARKET" {
                return self.place_market_order_optimized(symbol, side, quantity).await;
            }

            // Otherwise, use original implementation for LIMIT orders
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis() as u64;

            let mut params = vec![
                ("symbol", symbol.to_string()),
                ("side", side.to_string()),
                ("type", order_type.to_string()),
                ("quantity", quantity.to_string()),
                ("timestamp", timestamp.to_string()),
            ];
            
            if let Some(price_value) = price {
                params.push(("price", price_value.to_string()));
                params.push(("timeInForce", "IOC".to_string()));
            }

            params.sort_by(|a, b| a.0.cmp(b.0));
            
            let query_string = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");

            let signature = self.generate_signature(&query_string);
            let final_url = format!("{}/api/v3/order?{}&signature={}", 
                self.base_url, query_string, signature);
            
            let response = self.client
                .post(&final_url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()
                .await?;

            if response.status().is_success() {
                let result: Value = response.json().await?;
                Ok(result)
            } else {
                let error_text = response.text().await?;
                Err(format!("MEXC API Error: {}", error_text).into())
            }
        }

        pub async fn get_account_balance(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis() as u64;

            let query_string = format!("timestamp={}", timestamp);
            let signature = self.generate_signature(&query_string);
            let final_params = format!("{}&signature={}", query_string, signature);

            let url = format!("{}/api/v3/account?{}", self.base_url, final_params);
            
            let response = self.client
                .get(&url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()
                .await?;

            if response.status().is_success() {
                let result: Value = response.json().await?;
                Ok(result)
            } else {
                let error_text = response.text().await?;
                Err(format!("MEXC Balance Error: {}", error_text).into())
            }
        }

        pub async fn get_current_price(&self, symbol: &str) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            let url = format!("{}/api/v3/ticker/price?symbol={}", self.base_url, symbol);
            
            let response = self.client
                .get(&url)
                .header("X-MEXC-APIKEY", &self.api_key)
                .send()
                .await?;

            if response.status().is_success() {
                let result: serde_json::Value = response.json().await?;
                if let Some(price_str) = result.get("price").and_then(|p| p.as_str()) {
                    let price = price_str.parse::<Decimal>()
                        .map_err(|_| "Failed to parse price")?;
                    return Ok(price);
                }
            }
            
            Err("Failed to get current price".into())
        }

        // 🆕 OPTIMIZED: Enhanced version using optimized market orders
        pub async fn place_order_with_current_price(
            &self,
            symbol: &str,
            side: &str,
            order_type: &str,
            quantity: Decimal,
            _expected_price: Option<Decimal>,
        ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
            
            // Always use optimized market order for best execution
            if order_type == "MARKET" {
                return self.place_market_order_optimized(symbol, side, quantity).await;
            }
            
            // Fallback to original implementation for non-market orders
            let current_price = self.get_current_price(symbol).await?;
            log::info!("[MEXC] Current market price: ${:.4}", current_price);
            
            let execution_price = if side == "BUY" {
                current_price * dec!(1.002)
            } else {
                current_price * dec!(0.998)
            };
            
            self.place_order(symbol, side, order_type, quantity, Some(execution_price)).await
        }

        // 🆕 NEW: Pre-fetch orderbook for upcoming trades
        pub async fn prefetch_orderbook(&self, symbol: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // Fire and forget orderbook fetch to warm cache
            let _ = self.get_orderbook(symbol, 1).await;
            Ok(())
        }
    }
}
// 🚀 COMPLETE ENHANCED SOLANA TRADING MODULE - WITH TRANSACTION CONFIRMATION
mod solana_trading {
    use solana_sdk::{
        signer::{keypair::Keypair, Signer},
        commitment_config::CommitmentConfig,
        signature::Signature,
    };
    use solana_client::rpc_client::RpcClient;
    use std::sync::Arc;
    use base64::Engine;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rust_decimal::prelude::*;  // ✅ Provides FromPrimitive trait
    use std::time::{Duration, Instant};
    use tokio::time::sleep;
    use crate::config;  // ✅ Import config module

    #[derive(Debug, Clone)]
    pub struct SwapPriceInfo {
        pub executed_price: Decimal,
        pub slippage_percent: Decimal,
    }

    // ✅ NEW: Transaction result with confirmation data
    #[derive(Debug, Clone)]
    pub struct TransactionResult {
        pub signature: String,
        pub confirmed: bool,
        pub confirmation_time_ms: u64,
        pub final_balance_usdc: Option<Decimal>,
        pub final_balance_sol: Option<Decimal>,
    }

    #[derive(Clone)]
    pub struct SolanaTradingClient {
        client: Arc<RpcClient>,
        wallet: Arc<Keypair>,
        http_client: reqwest::Client,
        last_rpc_call: Arc<std::sync::Mutex<Instant>>,
        min_rpc_interval: Duration,
    }

    impl SolanaTradingClient {
        pub fn new(rpc_url: String, wallet_path: String, jupiter_api_key: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let client = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()));
            
            let wallet_data = std::fs::read_to_string(wallet_path)?;
            let wallet_bytes: Vec<u8> = serde_json::from_str(&wallet_data)?;
            let wallet = Arc::new(Keypair::from_bytes(&wallet_bytes)?);

            // HTTP client with x-api-key header
            let http_client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .pool_idle_timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(50)
                .tcp_keepalive(Duration::from_secs(30))
                .default_headers({
                    let mut headers = reqwest::header::HeaderMap::new();
                    // CRITICAL: Use lowercase x-api-key
                    headers.insert("x-api-key", reqwest::header::HeaderValue::from_str(&jupiter_api_key).unwrap());
                    headers.insert("Content-Type", reqwest::header::HeaderValue::from_str("application/json").unwrap());
                    headers
                })
                .build()?;

            log::info!("[SOLANA] 🚀 HTTP client initialized with Jupiter Pro API");

            Ok(Self {
                client,
                wallet,
                http_client,
                last_rpc_call: Arc::new(std::sync::Mutex::new(Instant::now() - Duration::from_secs(1))),
                min_rpc_interval: Duration::from_millis(10),
            })
        }

        pub fn get_wallet_address(&self) -> String {
            self.wallet.pubkey().to_string()
        }

        // ✅ ENHANCED: Wait for transaction confirmation
        pub async fn wait_for_confirmation(
            &self,
            signature: &Signature,
            timeout_seconds: u64,
        ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
            let start = Instant::now();
            let timeout = Duration::from_secs(timeout_seconds);
            
            log::info!("[SOLANA] ⏳ Waiting for transaction confirmation: {}", signature);
            
            while start.elapsed() < timeout {
                // Check rate limit
                let wait_time = self.rate_limit_check().await;
                if wait_time > Duration::from_millis(0) {
                    sleep(wait_time).await;
                }
                
                // Update timestamp
                self.update_rate_limit_timestamp();
                
                match self.client.get_signature_status(signature) {
                    Ok(Some(status)) => {
                        match status {
                            Ok(_) => {
                                log::info!("[SOLANA] ✅ Transaction confirmed in {}ms: {}", 
                                    start.elapsed().as_millis(), signature);
                                return Ok(true);
                            },
                            Err(e) => {
                                log::error!("[SOLANA] ❌ Transaction failed: {} - Error: {:?}", signature, e);
                                return Ok(false);
                            }
                        }
                    },
                    Ok(None) => {
                        // Transaction not found yet, continue waiting
                        log::debug!("[SOLANA] ⏳ Transaction not found yet, waiting... ({}ms elapsed)", 
                            start.elapsed().as_millis());
                    },
                    Err(e) => {
                        log::warn!("[SOLANA] ⚠️ Error checking transaction status: {}", e);
                    }
                }
                
                // Wait before checking again
                sleep(Duration::from_millis(500)).await;
            }
            
            log::error!("[SOLANA] ⏰ Transaction confirmation timeout after {}s: {}", timeout_seconds, signature);
            Ok(false)
        }

        // ✅ ENHANCED: Execute swap with confirmation and balance verification
        pub async fn execute_swap_with_confirmation(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<TransactionResult, Box<dyn std::error::Error + Send + Sync>> {
            
            let start_time = Instant::now();
            let wallet_address = self.get_wallet_address();
            
            log::info!("[SOLANA] 🚀 Starting confirmed swap: {} {} to {}", amount, 
                if input_mint.contains("So1111") { "lamports SOL" } else { "lamports USDC" },
                if output_mint.contains("So1111") { "SOL" } else { "USDC" });
            
            // Get balances before swap
            let (initial_usdc, initial_sol) = self.get_both_balances(&wallet_address).await?;
            log::info!("[SOLANA] 📊 Initial balances: ${:.4} USDC, {:.6} SOL", initial_usdc, initial_sol);
            
            // Execute the swap
            let signature_str = self.execute_swap(input_mint, output_mint, amount, slippage_bps).await?;
            let signature = signature_str.parse::<Signature>()
                .map_err(|e| format!("Invalid signature format: {}", e))?;
            
            // Wait for confirmation
            let confirmed = self.wait_for_confirmation(&signature, 30).await?;
            let confirmation_time = start_time.elapsed().as_millis() as u64;
            
            if !confirmed {
                return Ok(TransactionResult {
                    signature: signature_str,
                    confirmed: false,
                    confirmation_time_ms: confirmation_time,
                    final_balance_usdc: None,
                    final_balance_sol: None,
                });
            }
            
            // Wait additional time for balance updates to propagate
            log::info!("[SOLANA] ⏳ Waiting for balance updates to propagate...");
            sleep(Duration::from_secs(2)).await;
            
            // Get balances after swap
            let (final_usdc, final_sol) = self.get_both_balances(&wallet_address).await?;
            log::info!("[SOLANA] 📊 Final balances: ${:.4} USDC, {:.6} SOL", final_usdc, final_sol);
            
            // Log the actual changes
            let usdc_change = final_usdc - initial_usdc;
            let sol_change = final_sol - initial_sol;
            log::info!("[SOLANA] 🔄 Balance changes: USDC {:+.4}, SOL {:+.6}", usdc_change, sol_change);
            
            Ok(TransactionResult {
                signature: signature_str,
                confirmed: true,
                confirmation_time_ms: confirmation_time,
                final_balance_usdc: Some(final_usdc),
                final_balance_sol: Some(final_sol),
            })
        }

        // ✅ NEW: Get both USDC and SOL balances efficiently
        async fn get_both_balances(&self, wallet_address: &str) -> Result<(Decimal, Decimal), Box<dyn std::error::Error + Send + Sync>> {
            let usdc_future = self.get_usdc_balance(wallet_address);
            
            // ✅ FIXED: Create pubkey first to avoid temporary value issue
            let wallet_pubkey = wallet_address.parse::<solana_sdk::pubkey::Pubkey>()?;
            let sol_future = self.get_sol_balance(&wallet_pubkey);
            
            let (usdc_result, sol_result) = tokio::join!(usdc_future, sol_future);
            
            let usdc_balance = usdc_result?;
            let sol_balance = Decimal::from(sol_result?) / dec!(1_000_000_000);
            
            Ok((usdc_balance, sol_balance))
        }

        // ✅ UPGRADED: Rate limit optimized for Jupiter Pro 600 RPM
        async fn rate_limit_check(&self) -> Duration {
            let last_call = self.last_rpc_call.lock().unwrap();
            let elapsed = last_call.elapsed();
            
            if elapsed < self.min_rpc_interval {
                let wait_time = self.min_rpc_interval.saturating_sub(elapsed);
                log::debug!("[SOLANA] 🚀 Rate limiting: waiting {}ms (Jupiter Pro 600 RPM optimized)", wait_time.as_millis());
                wait_time
            } else {
                Duration::from_millis(0) // No wait needed
            }
        }

        // ✅ HELPER: Update rate limit timestamp
        fn update_rate_limit_timestamp(&self) {
            let mut last_call = self.last_rpc_call.lock().unwrap();
            *last_call = Instant::now();
        }

        // 🔧 OPTIMIZED: Rate-limited RPC calls
        async fn get_latest_blockhash_with_rate_limit(&self) -> Result<solana_sdk::hash::Hash, Box<dyn std::error::Error + Send + Sync>> {
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.get_latest_blockhash() {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Blockhash rate limited, retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await;  // ✅ FASTER: 0.5s retries
                            continue;
                        }
                        return Err(format!("Get blockhash failed: {}", e).into());
                    }
                }
            }
        }

        async fn send_transaction_with_rate_limit(&self, transaction: &solana_sdk::transaction::VersionedTransaction) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error + Send + Sync>> {
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.send_transaction(transaction) {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Send transaction rate limited, retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        return Err(format!("Send transaction failed: {}", e).into());
                    }
                }
            }
        }

        async fn send_legacy_transaction_with_rate_limit(&self, transaction: &solana_sdk::transaction::Transaction) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error + Send + Sync>> {
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.send_transaction(transaction) {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Send legacy transaction rate limited, retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        return Err(format!("Send legacy transaction failed: {}", e).into());
                    }
                }
            }
        }

        // ✅ PUBLIC: Get SOL balance (used by balance fetcher)
        pub async fn get_sol_balance(
            &self,
            pubkey: &solana_sdk::pubkey::Pubkey
        ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
            
            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Execute with retry logic
            let mut retries = 3;
            loop {
                match self.client.get_balance(pubkey) {
                    Ok(balance) => return Ok(balance),
                    Err(e) => {
                        if retries > 0 && e.to_string().contains("429") {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Balance fetch rate limited, retrying in 0.5s... ({} left)", retries);
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        return Err(format!("Failed to get SOL balance: {}", e).into());
                    }
                }
            }
        }

        // ✅ PUBLIC: Get USDC balance (SINGLE IMPLEMENTATION)
        pub async fn get_usdc_balance(
            &self,
            wallet_address: &str
        ) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            
            let request_body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    wallet_address,
                    {
                        "mint": config::USDC_MINT_ADDRESS
                    },
                    {
                        "encoding": "jsonParsed"
                    }
                ]
            });

            // Check rate limit
            let wait_time = self.rate_limit_check().await;
            if wait_time > Duration::from_millis(0) {
                sleep(wait_time).await;
            }

            // Update timestamp
            self.update_rate_limit_timestamp();

            // Use Helius RPC for better performance
            let app_config = config::AppConfig::new();
            let response = self.http_client
                .post(&app_config.solana_rpc_url)  // ✅ Use your Helius RPC
                .json(&request_body)
                .send()
                .await?;

            if !response.status().is_success() {
                return Err("Failed to fetch USDC balance".into());
            }

            let data: serde_json::Value = response.json().await?;
            
            if let Some(accounts) = data["result"]["value"].as_array() {
                if let Some(account) = accounts.first() {
                    if let Some(balance_value) = account["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"].as_f64() {
                        return Ok(Decimal::from_f64(balance_value).unwrap_or(Decimal::ZERO));  // ✅ FIXED: FromPrimitive trait
                    }
                }
            }
            
            // If no USDC token account exists, balance is 0
            Ok(Decimal::ZERO)
        }

        // ✅ KEEP: Original execute_swap method for compatibility
        pub async fn execute_swap(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            
            log::info!("[SOLANA] 🚀 Starting Jupiter swap: {} {} to {}", amount, 
                if input_mint.contains("So1111") { "lamports SOL" } else { "lamports USDC" },
                if output_mint.contains("So1111") { "SOL" } else { "USDC" });
            
            // 🔧 STEP 1: Get quote with retry logic
            let quote_data = self.get_jupiter_quote(input_mint, output_mint, amount, slippage_bps).await?;
            
            // 🔧 STEP 2: Get swap transaction with retry logic  
            let transaction_data = self.get_jupiter_swap_transaction(&quote_data).await?;
            
            // 🔧 STEP 3: Process and send transaction with rate limiting
            self.process_and_send_transaction(&transaction_data).await
        }

        async fn get_jupiter_quote(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
            
            let _app_config = config::AppConfig::new();
            // 🔧 FIXED: Use 'token' parameter instead of 'api-key'
            let quote_url = format!(
                "https://api.jup.ag/swap/v1/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}&onlyDirectRoutes=true&maxAccounts=20",
                input_mint, output_mint, amount, slippage_bps
            );

            log::debug!("[SOLANA] 🚀 Getting Jupiter quote with Pro API (3000 RPM) - Key in URL");

            let mut retries = 5;
            loop {
                let response = tokio::time::timeout(
                    Duration::from_secs(8),
                    self.http_client.get(&quote_url).send()
                ).await;

                match response {
                    Ok(Ok(resp)) if resp.status().is_success() => {
                        let quote_data: serde_json::Value = resp.json().await
                            .map_err(|e| format!("Failed to parse quote response: {}", e))?;
                        log::debug!("[SOLANA] ✅ Jupiter Pro quote received");
                        return Ok(quote_data);
                    },
                    Ok(Ok(resp)) => {
                        let status = resp.status();
                        let error_text = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                        
                        if status.as_u16() == 429 && retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter rate limited, retrying in 0.1s... ({} retries left)", retries);
                            sleep(Duration::from_millis(100)).await; // Pro plan can retry faster
                            continue;
                        }
                        
                        return Err(format!("Jupiter Pro quote failed ({}): {}", status, error_text).into());
                    },
                    Ok(Err(e)) => {
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro quote request failed, retrying... Error: {}", e);
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        return Err(format!("Jupiter Pro quote request failed: {}", e).into());
                    },
                    Err(_) => {
                        // Timeout error
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro quote timeout, retrying...");
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        return Err("Jupiter Pro quote timeout".into());
                    }
                }
            }
        }

        async fn get_jupiter_swap_transaction(
            &self,
            quote_data: &serde_json::Value,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            
            let swap_request = serde_json::json!({
                "userPublicKey": self.wallet.pubkey().to_string(),
                "quoteResponse": quote_data,
                "wrapAndUnwrapSol": true,
                "dynamicComputeUnitLimit": true,
                "prioritizationFeeLamports": 20000,  // 🚀 INCREASED: Even higher priority to reduce failures
                "asLegacyTransaction": false,
                "skipUserAccountsRpcCalls": true,
            });

            log::debug!("[SOLANA] 🚀 Getting swap transaction with Pro API (high priority)");

            // ✅ UPGRADED: Retry logic optimized for Pro plan
            let mut retries = 5; // More retries since we have higher limits
            loop {
                let response = tokio::time::timeout(
                    Duration::from_secs(10),
                    self.http_client
                        .post("https://api.jup.ag/swap/v1/swap")
                        .json(&swap_request)
                        .send() // ✅ Pro API key automatically included in headers
                ).await;

                match response {
                    Ok(Ok(resp)) if resp.status().is_success() => {
                        let swap_data: serde_json::Value = resp.json().await
                            .map_err(|e| format!("Failed to parse swap response: {}", e))?;
                        
                        let transaction_data = swap_data["swapTransaction"].as_str()
                            .ok_or("No swap transaction in response")?;
                        
                        log::debug!("[SOLANA] ✅ Jupiter Pro swap transaction received, size: {} chars", transaction_data.len());
                        return Ok(transaction_data.to_string());
                    },
                    Ok(Ok(resp)) => {
                        let status = resp.status();
                        let error_text = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                        
                        if status.as_u16() == 429 && retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro swap rate limited (unusual!), retrying in 0.5s... ({} retries left)", retries);
                            sleep(Duration::from_millis(500)).await; // ✅ FASTER: 0.5s retries for Pro
                            continue;
                        }
                        
                        return Err(format!("Jupiter Pro swap failed ({}): {}", status, error_text).into());
                    },
                    Ok(Err(e)) => {
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro swap request failed, retrying... Error: {}", e);
                            sleep(Duration::from_millis(200)).await; // ✅ FASTER: 0.2s retries
                            continue;
                        }
                        return Err(format!("Jupiter Pro swap request failed: {}", e).into());
                    },
                    Err(_) => {
                        if retries > 0 {
                            retries -= 1;
                            log::warn!("[SOLANA] 🔄 Jupiter Pro swap timeout, retrying...");
                            sleep(Duration::from_millis(200)).await; // ✅ FASTER: 0.2s retries
                            continue;
                        }
                        return Err("Jupiter Pro swap timeout".into());
                    }
                }
            }
        }

        async fn process_and_send_transaction(
            &self,
            transaction_data: &str,
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            
            let transaction_bytes = base64::prelude::BASE64_STANDARD.decode(transaction_data)
                .map_err(|e| format!("Failed to decode transaction: {}", e))?;

            log::debug!("[SOLANA] 🔧 Transaction bytes decoded, size: {} bytes", transaction_bytes.len());

            if transaction_bytes.len() > 1232 {
                return Err("Transaction too large for Solana network".into());
            }

            // Try versioned transaction first
            match bincode::deserialize::<solana_sdk::transaction::VersionedTransaction>(&transaction_bytes) {
                Ok(mut versioned_tx) => {
                    log::debug!("[SOLANA] ✅ Using versioned transaction");
                    
                    // 🔧 FIXED: Use specific rate-limited function
                    let recent_blockhash = self.get_latest_blockhash_with_rate_limit().await?;
                    
                    // Set blockhash
                    match &mut versioned_tx.message {
                        solana_sdk::message::VersionedMessage::Legacy(ref mut legacy_msg) => {
                            legacy_msg.recent_blockhash = recent_blockhash;
                        },
                        solana_sdk::message::VersionedMessage::V0(ref mut v0_msg) => {
                            v0_msg.recent_blockhash = recent_blockhash;
                        }
                    }
                    
                    // Sign transaction
                    let signed_tx = solana_sdk::transaction::VersionedTransaction::try_new(
                        versioned_tx.message.clone(),
                        &[self.wallet.as_ref()]
                    ).map_err(|e| format!("Failed to sign versioned transaction: {}", e))?;
                    
                    // 🔧 FIXED: Use specific rate-limited function
                    let signature = self.send_transaction_with_rate_limit(&signed_tx).await?;
                    
                    log::info!("[SOLANA] ✅ Versioned transaction sent! Signature: {}", signature);
                    Ok(signature.to_string())
                },
                Err(_) => {
                    // Fallback to legacy transaction
                    log::debug!("[SOLANA] 🔄 Falling back to legacy transaction");
                    let mut transaction: solana_sdk::transaction::Transaction = bincode::deserialize(&transaction_bytes)
                        .map_err(|e| format!("Failed to deserialize legacy transaction: {}", e))?;
                    
                    // 🔧 FIXED: Use specific rate-limited function
                    let recent_blockhash = self.get_latest_blockhash_with_rate_limit().await?;
                    
                    transaction.message.recent_blockhash = recent_blockhash;
                    transaction.try_sign(&[self.wallet.as_ref()], recent_blockhash)
                        .map_err(|e| format!("Failed to sign transaction: {}", e))?;

                    // 🔧 FIXED: Use specific rate-limited function
                    let signature = self.send_legacy_transaction_with_rate_limit(&transaction).await?;
                    
                    log::info!("[SOLANA] ✅ Legacy transaction sent! Signature: {}", signature);
                    Ok(signature.to_string())
                }
            }
        }

        pub async fn execute_swap_with_price_tracking(
            &self,
            input_mint: &str,
            output_mint: &str,
            amount: u64,
            slippage_bps: u16,
        ) -> Result<(String, SwapPriceInfo), Box<dyn std::error::Error + Send + Sync>> {
            
            log::info!("[SOLANA] 🚀 Starting Jupiter swap with price tracking: {} {} to {}", amount, 
                if input_mint.contains("So1111") { "lamports SOL" } else { "lamports USDC" },
                if output_mint.contains("So1111") { "SOL" } else { "USDC" });
            
            // Get quote for price calculation
            let quote_data = self.get_jupiter_quote(input_mint, output_mint, amount, slippage_bps).await?;
            
            // Extract expected amounts for price calculation
            let in_amount = quote_data["inAmount"].as_str()
                .ok_or("Missing inAmount")?.parse::<u64>()?;
            let out_amount = quote_data["outAmount"].as_str()
                .ok_or("Missing outAmount")?.parse::<u64>()?;

            log::debug!("[SOLANA] Quote: {} -> {} (expected)", in_amount, out_amount);

            // Execute the swap
            let signature = self.execute_swap(input_mint, output_mint, amount, slippage_bps).await?;
            
            // Calculate executed price
            let executed_price = if input_mint.contains("So1111") {
                // SOL -> USDC: price = USDC_out / SOL_in
                Decimal::from(out_amount) / Decimal::from(1_000_000) / (Decimal::from(in_amount) / Decimal::from(1_000_000_000))
            } else {
                // USDC -> SOL: price = USDC_in / SOL_out  
                Decimal::from(in_amount) / Decimal::from(1_000_000) / (Decimal::from(out_amount) / Decimal::from(1_000_000_000))
            };
            
            let price_info = SwapPriceInfo {
                executed_price,
                slippage_percent: Decimal::from(slippage_bps) / dec!(100.0),
            };
            
            log::info!("[SOLANA] ✅ Swap with tracking executed at ${:.4} per SOL", executed_price);
            Ok((signature, price_info))
        }
    }
}
// ---- FIXED Shared State Module with Trading-Based P&L ----
mod state {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use chrono::{DateTime, Utc};
    use serde::Serialize;
    use crate::config;

    pub type Price = Decimal;

    #[derive(Debug, Clone, Copy, Serialize)]
    pub struct PriceInfo {
        pub price: Price,
        pub timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct PriceDataPoint {
        pub timestamp: DateTime<Utc>,
        pub source: String,
        pub price: Price,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct TradeRecord {
        pub trade_id: String,
        pub timestamp: DateTime<Utc>,
        pub direction: String,
        pub amount_sol: Decimal,
        pub buy_venue: String,
        pub sell_venue: String,
        pub buy_price: Decimal,
        pub sell_price: Decimal,
        pub expected_profit: Decimal,
        pub actual_profit: Option<Decimal>,
        pub expected_spread_percent: Decimal,
        pub actual_spread_percent: Option<Decimal>,
        pub buy_slippage_percent: Option<Decimal>,
        pub sell_slippage_percent: Option<Decimal>,
        pub total_slippage_percent: Option<Decimal>,
        pub buy_order_id: Option<String>,
        pub sell_order_id: Option<String>,
        pub solana_tx_signature: Option<String>,
        pub total_fees: Decimal,
        pub execution_time_ms: Option<u64>,
        pub status: String,
        pub actual_buy_price: Option<Decimal>,
        pub actual_sell_price: Option<Decimal>,
    }

    #[derive(Debug, Clone)]
    pub struct AccountBalance {
        pub mexc_usdc: Decimal,
        pub mexc_sol: Decimal,     
        pub wallet_usdc: Decimal,
        pub wallet_sol: Decimal,   
    }

    #[derive(Debug, Clone)]
    pub struct SharedState {
        pub mexc_price: Option<PriceInfo>,
        pub solana_price: Option<PriceInfo>,
        pub starting_capital: Decimal,
        pub session_start_capital: Decimal,
        pub balances: AccountBalance,
        pub price_history: Vec<PriceDataPoint>,
        pub trade_history: Vec<TradeRecord>,
        pub total_profit: Decimal,
        pub trade_count: u32,
        pub last_rebalance_check: chrono::DateTime<chrono::Utc>,
        
        // ✅ NEW: Trading-specific P&L tracking
        pub trading_pnl: Decimal,           // Cumulative trading profit/loss
        pub trading_fees_paid: Decimal,     // Total fees paid for trading
        pub successful_trades: u32,         // Count of successful trades
        pub failed_trades: u32,             // Count of failed trades
        pub largest_win: Decimal,           // Largest single trade profit
        pub largest_loss: Decimal,          // Largest single trade loss
        pub trading_start_time: DateTime<Utc>, // When trading started
    }

    impl SharedState {
        pub fn new_with_live_balances(
            mexc_usdc: Decimal, 
            mexc_sol: Decimal,
            wallet_usdc: Decimal,
            wallet_sol: Decimal,
            total_starting_capital: Decimal
        ) -> Self {
            let balances = AccountBalance {
                mexc_usdc,
                mexc_sol,
                wallet_usdc,
                wallet_sol,
            };
            
            log::info!("[STATE] 🆕 Initialized with complete live balances:");
            log::info!("[STATE] 💰 MEXC - USDC: ${:.4}, SOL: {:.6}", mexc_usdc, mexc_sol);
            log::info!("[STATE] 🏦 Wallet - USDC: ${:.4}, SOL: {:.6}", wallet_usdc, wallet_sol);
            log::info!("[STATE] 💎 Total Starting Capital: ${:.4}", total_starting_capital);
            
            Self {
                mexc_price: None,
                solana_price: None,
                starting_capital: total_starting_capital,
                session_start_capital: total_starting_capital,
                balances,
                price_history: Vec::new(),
                trade_history: Vec::new(),
                total_profit: Decimal::ZERO,
                trade_count: 0,
                last_rebalance_check: chrono::Utc::now(),
                
                // ✅ NEW: Initialize trading P&L tracking
                trading_pnl: Decimal::ZERO,
                trading_fees_paid: Decimal::ZERO,
                successful_trades: 0,
                failed_trades: 0,
                largest_win: Decimal::ZERO,
                largest_loss: Decimal::ZERO,
                trading_start_time: chrono::Utc::now(),
            }
        }

        pub fn new() -> Self {
            log::warn!("[STATE] ⚠️ Using legacy constructor - should use new_with_live_balances()");
            
            let balances = AccountBalance {
                mexc_usdc: config::MEXC_STARTING_USDC,
                mexc_sol: Decimal::ZERO,
                wallet_usdc: config::WALLET_STARTING_USDC,
                wallet_sol: Decimal::ZERO,
            };
            let starting_capital = balances.mexc_usdc + balances.wallet_usdc;
            
            Self {
                mexc_price: None,
                solana_price: None,
                starting_capital,
                session_start_capital: starting_capital,
                balances,
                price_history: Vec::new(),
                trade_history: Vec::new(),
                total_profit: Decimal::ZERO,
                trade_count: 0,
                last_rebalance_check: chrono::Utc::now(),
                
                // ✅ NEW: Initialize trading P&L tracking
                trading_pnl: Decimal::ZERO,
                trading_fees_paid: Decimal::ZERO,
                successful_trades: 0,
                failed_trades: 0,
                largest_win: Decimal::ZERO,
                largest_loss: Decimal::ZERO,
                trading_start_time: chrono::Utc::now(),
            }
        }
        
        pub fn update_live_balances(
            &mut self, 
            mexc_usdc: Decimal, 
            mexc_sol: Decimal,
            wallet_usdc: Decimal,
            wallet_sol: Decimal,
            sol_price: Decimal
        ) {
            let old_capital = self.get_current_capital(sol_price);
            
            self.balances.mexc_usdc = mexc_usdc;
            self.balances.mexc_sol = mexc_sol;
            self.balances.wallet_usdc = wallet_usdc;
            self.balances.wallet_sol = wallet_sol;
            
            let new_capital = self.get_current_capital(sol_price);
            let capital_change = new_capital - old_capital;
            
            log::debug!("[STATE] 💰 Balance update: Capital changed by ${:+.4}", capital_change);
            log::debug!("[STATE] 📊 New total capital: ${:.4}", new_capital);
        }

        pub fn get_current_capital(&self, sol_price: Decimal) -> Decimal {
            self.balances.mexc_usdc + 
            (self.balances.mexc_sol * sol_price) +
            self.balances.wallet_usdc + 
            (self.balances.wallet_sol * sol_price)
        }

        pub fn get_current_capital_legacy(&self) -> Decimal {
            self.balances.mexc_usdc + self.balances.wallet_usdc
        }

        // ✅ UPDATED: Enhanced trade record tracking
        pub fn add_trade_record(&mut self, trade: TradeRecord) {
            // Update trading P&L based on actual results
            if let Some(actual_profit) = trade.actual_profit {
                // Update cumulative trading P&L
                self.trading_pnl += actual_profit;
                self.total_profit += actual_profit;
                
                // Update trading fees
                self.trading_fees_paid += trade.total_fees;
                
                // Track win/loss records
                if actual_profit > Decimal::ZERO {
                    self.successful_trades += 1;
                    if actual_profit > self.largest_win {
                        self.largest_win = actual_profit;
                    }
                } else if actual_profit < Decimal::ZERO {
                    if actual_profit < self.largest_loss {
                        self.largest_loss = actual_profit;
                    }
                }
                
                log::debug!("[STATE] 📊 Trade P&L updated: ${:+.4} (Session: ${:+.4})", 
                    actual_profit, self.trading_pnl);
            }
            
            // Update trade counts
            if trade.status == "SUCCESS" {
                self.successful_trades += 1;
            } else {
                self.failed_trades += 1;
            }
            
            self.trade_count += 1;
            self.trade_history.push(trade);
        }

        // ✅ NEW: Get trading-based P&L (not market-based)
        pub fn get_trading_pnl(&self) -> Decimal {
            self.trading_pnl
        }

        // ✅ NEW: Get trading-based P&L percentage
        pub fn get_trading_pnl_percent(&self) -> Decimal {
            if self.session_start_capital.is_zero() {
                Decimal::ZERO
            } else {
                (self.trading_pnl / self.session_start_capital) * dec!(100.0)
            }
        }

        // ✅ DEPRECATED: Old session P&L methods (keep for backward compatibility)
        pub fn get_session_pnl(&self, sol_price: Decimal) -> Decimal {
            log::warn!("[STATE] ⚠️ get_session_pnl() is deprecated. Use get_trading_pnl() instead.");
            self.get_current_capital(sol_price) - self.session_start_capital
        }

        pub fn get_session_pnl_percent(&self, sol_price: Decimal) -> Decimal {
            log::warn!("[STATE] ⚠️ get_session_pnl_percent() is deprecated. Use get_trading_pnl_percent() instead.");
            let pnl = self.get_session_pnl(sol_price);
            if self.session_start_capital.is_zero() {
                Decimal::ZERO
            } else {
                (pnl / self.session_start_capital) * dec!(100.0)
            }
        }

        // ✅ NEW: Get trading statistics
        pub fn get_trading_stats(&self) -> TradingStats {
            let total_trades = self.successful_trades + self.failed_trades;
            let success_rate = if total_trades > 0 {
                (self.successful_trades as f64 / total_trades as f64) * 100.0
            } else {
                0.0
            };
            
            let avg_profit_per_trade = if self.successful_trades > 0 {
                self.trading_pnl / Decimal::from(self.successful_trades)
            } else {
                Decimal::ZERO
            };
            
            TradingStats {
                total_trades,
                successful_trades: self.successful_trades,
                failed_trades: self.failed_trades,
                success_rate,
                trading_pnl: self.trading_pnl,
                trading_pnl_percent: self.get_trading_pnl_percent(),
                total_fees_paid: self.trading_fees_paid,
                largest_win: self.largest_win,
                largest_loss: self.largest_loss,
                avg_profit_per_trade,
                trading_duration: chrono::Utc::now().signed_duration_since(self.trading_start_time),
            }
        }

        pub fn are_prices_fresh(&self) -> bool {
            let now = Utc::now();
            let max_age = chrono::Duration::seconds(config::MAX_PRICE_AGE_SECONDS as i64);
            
            match (&self.mexc_price, &self.solana_price) {
                (Some(mexc), Some(solana)) => {
                    let mexc_age = now.signed_duration_since(mexc.timestamp);
                    let solana_age = now.signed_duration_since(solana.timestamp);
                    mexc_age < max_age && solana_age < max_age
                },
                _ => false
            }
        }

        // Auto-rebalancing methods (unchanged)
        pub fn needs_rebalancing(&self, sol_price: Decimal) -> Option<RebalanceAction> {
            if !config::AUTO_REBALANCE_ENABLED {
                return None;
            }

            let mexc_action = self.check_mexc_rebalance_need(sol_price);
            let wallet_action = self.check_wallet_rebalance_need(sol_price);

            mexc_action.or(wallet_action)
        }

        fn check_mexc_rebalance_need(&self, sol_price: Decimal) -> Option<RebalanceAction> {
            let mexc_usdc_value = self.balances.mexc_usdc;
            let mexc_sol_value = self.balances.mexc_sol * sol_price;
            let mexc_total = mexc_usdc_value + mexc_sol_value;

            if mexc_total < config::REBALANCE_MIN_VALUE_USD {
                return None;
            }

            let usdc_percentage = (mexc_usdc_value / mexc_total) * dec!(100.0);
            let sol_percentage = (mexc_sol_value / mexc_total) * dec!(100.0);

            if usdc_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                let target_usdc_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let usdc_to_convert = mexc_usdc_value - target_usdc_value;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::MEXC,
                    action_type: RebalanceType::UsdcToSol,
                    amount: usdc_to_convert,
                    reason: format!("MEXC USDC: {:.1}% (target: 50%)", usdc_percentage),
                })
            } else if sol_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                let target_sol_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let sol_to_convert = (mexc_sol_value - target_sol_value) / sol_price;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::MEXC,
                    action_type: RebalanceType::SolToUsdc,
                    amount: sol_to_convert,
                    reason: format!("MEXC SOL: {:.1}% (target: 50%)", sol_percentage),
                })
            } else {
                None
            }
        }

        fn check_wallet_rebalance_need(&self, sol_price: Decimal) -> Option<RebalanceAction> {
            let wallet_usdc_value = self.balances.wallet_usdc;
            let wallet_sol_value = self.balances.wallet_sol * sol_price;
            let wallet_total = wallet_usdc_value + wallet_sol_value;

            if wallet_total < config::REBALANCE_MIN_VALUE_USD {
                return None;
            }

            let usdc_percentage = (wallet_usdc_value / wallet_total) * dec!(100.0);
            let sol_percentage = (wallet_sol_value / wallet_total) * dec!(100.0);

            if usdc_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                let target_usdc_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let usdc_to_convert = wallet_usdc_value - target_usdc_value;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::Wallet,
                    action_type: RebalanceType::UsdcToSol,
                    amount: usdc_to_convert,
                    reason: format!("Wallet USDC: {:.1}% (target: 50%)", usdc_percentage),
                })
            } else if sol_percentage >= config::REBALANCE_THRESHOLD_PERCENT {
                let target_sol_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
                let sol_to_convert = (wallet_sol_value - target_sol_value) / sol_price;
                
                Some(RebalanceAction {
                    venue: RebalanceVenue::Wallet,
                    action_type: RebalanceType::SolToUsdc,
                    amount: sol_to_convert,
                    reason: format!("Wallet SOL: {:.1}% (target: 50%)", sol_percentage),
                })
            } else {
                None
            }
        }

        pub fn update_rebalance_check_time(&mut self) {
            self.last_rebalance_check = chrono::Utc::now();
        }

        pub fn should_check_rebalance(&self) -> bool {
            let now = chrono::Utc::now();
            let elapsed = now.signed_duration_since(self.last_rebalance_check);
            elapsed.num_seconds() >= config::REBALANCE_CHECK_INTERVAL_SECONDS as i64
        }
    }

    // ✅ NEW: Trading statistics structure
    #[derive(Debug, Clone)]
    pub struct TradingStats {
        pub total_trades: u32,
        pub successful_trades: u32,
        pub failed_trades: u32,
        pub success_rate: f64,
        pub trading_pnl: Decimal,
        pub trading_pnl_percent: Decimal,
        pub total_fees_paid: Decimal,
        pub largest_win: Decimal,
        pub largest_loss: Decimal,
        pub avg_profit_per_trade: Decimal,
        pub trading_duration: chrono::Duration,
    }

    #[derive(Debug)]
    pub enum PriceUpdate {
        MEXC(Price),
        Solana(Price),
    }

    #[derive(Debug, Clone)]
    pub struct RebalanceAction {
        pub venue: RebalanceVenue,
        pub action_type: RebalanceType,
        pub amount: Decimal,
        pub reason: String,
    }

    #[derive(Debug, Clone)]
    pub enum RebalanceVenue {
        MEXC,
        Wallet,
    }

    #[derive(Debug, Clone)]
    pub enum RebalanceType {
        UsdcToSol,
        SolToUsdc,
    }
}
// 🔧 UPDATED RISK MODULE with Trading-Based P&L
mod risk {
    use crate::{config, state::{SharedState, TradeRecord}, mexc_trading::MexcTradingClient, solana_trading::{SolanaTradingClient, SwapPriceInfo}};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use chrono::Utc;
    use std::time::Instant;
    use rust_decimal::prelude::*;

    pub struct RiskManager;

    impl RiskManager {
        // ✅ UPDATED: Check trade using trading P&L instead of total capital changes
        pub fn check_trade(state: &SharedState, trade_value_usd: Decimal, current_sol_price: Decimal) -> bool {
            let current_capital = state.get_current_capital(current_sol_price);

            // Check maximum trade size (unchanged)
            let max_trade_value = current_capital * (config::MAX_CAPITAL_PER_TRADE_PERCENT / dec!(100.0));
            if trade_value_usd > max_trade_value {
                log::warn!("[RISK] VETO: Trade value ${:.4} exceeds max size ${:.4} ({}% of ${:.4})", 
                    trade_value_usd, max_trade_value, config::MAX_CAPITAL_PER_TRADE_PERCENT, current_capital);
                return false;
            }

            // ✅ FIXED: Use trading P&L instead of total capital changes
            let trading_pnl_percent = state.get_trading_pnl_percent();
            
            // Check if trading losses exceed drawdown limit
            if trading_pnl_percent < -config::MAX_DAILY_DRAWDOWN_PERCENT {
                log::error!("[RISK] 🚨 CRITICAL: Trading drawdown of {:.2}% exceeds limit of {:.2}%!", 
                    trading_pnl_percent.abs(), config::MAX_DAILY_DRAWDOWN_PERCENT);
                log::error!("[RISK] 📊 Trading P&L: ${:+.4} ({:+.2}%)", 
                    state.get_trading_pnl(), trading_pnl_percent);
                log::error!("[RISK] 📈 This is based on TRADING RESULTS, not market price changes");
                return false;
            }

            // ✅ NEW: Enhanced risk status logging
            if state.trade_count > 0 {
                log::debug!("[RISK] ✅ Trade approved - Size: ${:.4}/{:.4}, Trading P&L: {:+.2}%/{:.1}%", 
                    trade_value_usd, max_trade_value, trading_pnl_percent, config::MAX_DAILY_DRAWDOWN_PERCENT);
            } else {
                log::debug!("[RISK] ✅ First trade approved - Size: ${:.4}/{:.4}, No trading P&L yet", 
                    trade_value_usd, max_trade_value);
            }
            
            true
        }

        // ✅ ENHANCED: Check price freshness (unchanged)
        pub fn check_price_freshness(state: &SharedState) -> bool {
            if !state.are_prices_fresh() {
                log::warn!("[RISK] VETO: Prices are stale - aborting trade");
                return false;
            }
            true
        }

        // ✅ ENHANCED: Update capital for paper trading with SOL price awareness
        pub fn update_capital(
            state: &mut SharedState, 
            venue_bought: &str, 
            amount_sol: Decimal, 
            price_bought: Decimal, 
            price_sold: Decimal,
            _current_sol_price: Decimal
        ) {
            // 🚨 CRITICAL: This should only run in paper trading mode
            if !config::PAPER_TRADING {
                log::error!("[REAL_TRADE] ❌ update_capital() called but real trades should use execute_parallel_arbitrage()!");
                return;
            }

            // Paper trading logic (legacy USDC-only tracking)
            let cost = if venue_bought == "MEXC" {
                amount_sol * price_bought
            } else {
                (amount_sol * price_bought) + config::SOLANA_GAS_FEE_USD
            };

            let revenue = if venue_bought == "MEXC" {
                amount_sol * price_sold - config::SOLANA_GAS_FEE_USD
            } else {
                amount_sol * price_sold
            };

            let profit = revenue - cost;

            if venue_bought == "MEXC" {
                state.balances.mexc_usdc -= cost;
                state.balances.wallet_usdc += revenue;
            } else {
                state.balances.wallet_usdc -= cost;
                state.balances.mexc_usdc += revenue;
            }

            // ✅ NEW: Update trading P&L for paper trading
            state.trading_pnl += profit;
            state.total_profit += profit;

            log::info!("[PAPER_TRADE] 🚀 ZERO-FEE SOL/USDC Profit: ${:.4}. Trading P&L: ${:+.4}", 
                profit, state.trading_pnl);
        }

        // 🚀 COMPLETE Enhanced execution function with beautiful trade output
        pub async fn execute_parallel_arbitrage(
            mexc_client: &MexcTradingClient,
            solana_client: &SolanaTradingClient,
            venue_bought: &str,
            amount_sol: Decimal,
            expected_buy_price: Decimal,
            expected_sell_price: Decimal,
        ) -> Result<TradeRecord, Box<dyn std::error::Error + Send + Sync>> {
            
            let start_time = Instant::now();
            let trade_id = format!("TRADE_{}", Utc::now().format("%Y%m%d_%H%M%S_%3f"));
            
            log::info!("[PARALLEL] 🚀 Executing {} with PARALLEL execution", trade_id);
            log::info!("[PARALLEL] Direction: {}, Amount: {} SOL", venue_bought, amount_sol);
            
            let mut trade_record = TradeRecord {
                trade_id: trade_id.clone(),
                timestamp: Utc::now(),
                direction: if venue_bought == "MEXC" { 
                    "Buy MEXC -> Sell Solana".to_string() 
                } else { 
                    "Buy Solana -> Sell MEXC".to_string() 
                },
                amount_sol,
                buy_venue: venue_bought.to_string(),
                sell_venue: if venue_bought == "MEXC" { "Solana".to_string() } else { "MEXC".to_string() },
                buy_price: expected_buy_price,
                sell_price: expected_sell_price,
                expected_profit: amount_sol * (expected_sell_price - expected_buy_price) - config::SOLANA_GAS_FEE_USD,
                actual_profit: None,
                expected_spread_percent: ((expected_sell_price - expected_buy_price) / expected_buy_price) * dec!(100.0),
                actual_spread_percent: None,
                buy_slippage_percent: None,
                sell_slippage_percent: None,
                total_slippage_percent: None,
                buy_order_id: None,
                sell_order_id: None,
                solana_tx_signature: None,
                total_fees: config::SOLANA_GAS_FEE_USD,
                execution_time_ms: None,
                status: "EXECUTING".to_string(),
                actual_buy_price: None,
                actual_sell_price: None,
            };

            if config::PAPER_TRADING {
                trade_record.actual_profit = Some(trade_record.expected_profit);
                trade_record.actual_spread_percent = Some(trade_record.expected_spread_percent);
                trade_record.buy_slippage_percent = Some(dec!(0.01));
                trade_record.sell_slippage_percent = Some(dec!(0.015));
                trade_record.total_slippage_percent = Some(dec!(0.025));
                trade_record.execution_time_ms = Some(1200);
                trade_record.status = "SUCCESS".to_string();
                trade_record.actual_buy_price = Some(expected_buy_price * dec!(1.0001));
                trade_record.actual_sell_price = Some(expected_sell_price * dec!(0.9999));
                
                // 🎯 BEAUTIFUL TRADE OUTPUT - Paper Trading
                Self::display_trade_results(&trade_record);
                return Ok(trade_record);
            }

            // 🚀 REAL TRADING EXECUTION with detailed tracking
            let mut actual_buy_price = expected_buy_price;
            let mut actual_sell_price = expected_sell_price;

            if venue_bought == "MEXC" {
                // MEXC Buy + Solana Sell in parallel
                let mexc_buy_future = mexc_client.place_order_with_current_price(
                    "SOLUSDC", "BUY", "MARKET", amount_sol, Some(expected_buy_price)
                );
                
                let sol_lamports = (amount_sol * dec!(1_000_000_000)).to_u64().unwrap_or(0);
                log::info!("[PARALLEL] 🔧 FIXED: Converting {} SOL to {} lamports", amount_sol, sol_lamports);
                log::info!("[PARALLEL] MEXC BUY {} SOL + Solana SELL {} lamports", amount_sol, sol_lamports);
                
                let solana_sell_future = solana_client.execute_swap_with_price_tracking(
                    config::SOL_MINT_ADDRESS,
                    config::USDC_MINT_ADDRESS,
                    sol_lamports,
                    50, // 0.5% slippage for reliability
                );

                let (mexc_result, solana_result): (
                    Result<serde_json::Value, _>, 
                    Result<(String, SwapPriceInfo), _>
                ) = tokio::join!(mexc_buy_future, solana_sell_future);

                match (&mexc_result, &solana_result) {
                    (Ok(mexc_order), Ok((solana_tx, solana_price_info))) => {
                        log::info!("[PARALLEL] ✅ Both trades succeeded!");
                        
                        if let Some(order_id) = mexc_order.get("orderId").and_then(|id| id.as_str()) {
                            trade_record.buy_order_id = Some(order_id.to_string());
                        }
                        if let Some(fills) = mexc_order.get("fills").and_then(|f| f.as_array()) {
                            if let Some(fill) = fills.first() {
                                if let Some(price_str) = fill.get("price").and_then(|p| p.as_str()) {
                                    actual_buy_price = price_str.parse().unwrap_or(expected_buy_price);
                                }
                            }
                        }
                        
                        trade_record.solana_tx_signature = Some(solana_tx.clone());
                        actual_sell_price = solana_price_info.executed_price;
                        trade_record.actual_buy_price = Some(actual_buy_price);
                        trade_record.actual_sell_price = Some(actual_sell_price);
                    },
                    (Ok(_), Err(solana_err)) => {
                        log::error!("[PARALLEL] ❌ Solana SELL failed: {}", solana_err);
                        return Err(format!("Solana sell failed: {}", solana_err).into());
                    },
                    (Err(mexc_err), Ok(_)) => {
                        log::error!("[PARALLEL] ❌ MEXC BUY failed: {}", mexc_err);
                        return Err(format!("MEXC buy failed: {}", mexc_err).into());
                    },
                    (Err(mexc_err), Err(solana_err)) => {
                        log::error!("[PARALLEL] ❌ Both trades failed - MEXC: {}, Solana: {}", mexc_err, solana_err);
                        return Err(format!("Both trades failed").into());
                    }
                }
            } else {
                // Solana Buy + MEXC Sell in parallel
                let usdc_lamports = (amount_sol * expected_buy_price * dec!(1_000_000)).to_u64().unwrap_or(0);
                log::info!("[PARALLEL] 🔧 FIXED: Converting ${} to {} USDC lamports", amount_sol * expected_buy_price, usdc_lamports);
                log::info!("[PARALLEL] Solana BUY {} USDC lamports + MEXC SELL {} SOL", usdc_lamports, amount_sol);
                
                let solana_buy_future = solana_client.execute_swap_with_price_tracking(
                    config::USDC_MINT_ADDRESS,
                    config::SOL_MINT_ADDRESS,
                    usdc_lamports,
                    50, // 0.5% slippage for reliability
                );
                
                let mexc_sell_future = mexc_client.place_order_with_current_price(
                    "SOLUSDC", "SELL", "MARKET", amount_sol, Some(expected_sell_price)
                );

                let (solana_result, mexc_result): (
                    Result<(String, SwapPriceInfo), _>, 
                    Result<serde_json::Value, _>
                ) = tokio::join!(solana_buy_future, mexc_sell_future);

                match (&solana_result, &mexc_result) {
                    (Ok((solana_tx, solana_price_info)), Ok(mexc_order)) => {
                        log::info!("[PARALLEL] ✅ Both trades succeeded!");
                        trade_record.solana_tx_signature = Some(solana_tx.clone());
                        actual_buy_price = solana_price_info.executed_price;
                        
                        if let Some(order_id) = mexc_order.get("orderId").and_then(|id| id.as_str()) {
                            trade_record.sell_order_id = Some(order_id.to_string());
                        }
                        if let Some(fills) = mexc_order.get("fills").and_then(|f| f.as_array()) {
                            if let Some(fill) = fills.first() {
                                if let Some(price_str) = fill.get("price").and_then(|p| p.as_str()) {
                                    actual_sell_price = price_str.parse().unwrap_or(expected_sell_price);
                                }
                            }
                        }
                        
                        trade_record.actual_buy_price = Some(actual_buy_price);
                        trade_record.actual_sell_price = Some(actual_sell_price);
                    },
                    (Ok(_), Err(mexc_err)) => {
                        log::error!("[PARALLEL] ❌ MEXC SELL failed: {}", mexc_err);
                        return Err(format!("MEXC sell failed: {}", mexc_err).into());
                    },
                    (Err(solana_err), Ok(_)) => {
                        log::error!("[PARALLEL] ❌ Solana BUY failed: {}", solana_err);
                        return Err(format!("Solana buy failed: {}", solana_err).into());
                    },
                    (Err(solana_err), Err(mexc_err)) => {
                        log::error!("[PARALLEL] ❌ Both trades failed - Solana: {}, MEXC: {}", solana_err, mexc_err);
                        return Err(format!("Both trades failed").into());
                    }
                }
            }
            
            // 🆕 Calculate detailed metrics
            let execution_time = start_time.elapsed().as_millis() as u64;
            trade_record.execution_time_ms = Some(execution_time);
            
            // Calculate slippage
            let buy_slippage = ((actual_buy_price - expected_buy_price) / expected_buy_price).abs() * dec!(100.0);
            let sell_slippage = ((actual_sell_price - expected_sell_price) / expected_sell_price).abs() * dec!(100.0);
            let total_slippage = buy_slippage + sell_slippage;
            
            trade_record.buy_slippage_percent = Some(buy_slippage);
            trade_record.sell_slippage_percent = Some(sell_slippage);
            trade_record.total_slippage_percent = Some(total_slippage);
            
            // Calculate actual profit and spread
            let actual_profit = amount_sol * (actual_sell_price - actual_buy_price) - config::SOLANA_GAS_FEE_USD;
            let actual_spread = ((actual_sell_price - actual_buy_price) / actual_buy_price) * dec!(100.0);
            
            trade_record.actual_profit = Some(actual_profit);
            trade_record.actual_spread_percent = Some(actual_spread);
            trade_record.status = "SUCCESS".to_string();
            
            // 🎯 BEAUTIFUL TRADE OUTPUT - Real Trading
            Self::display_trade_results(&trade_record);
            
            log::info!("[PARALLEL] ✅ {} COMPLETED in {}ms!", trade_id, execution_time);
            Ok(trade_record)
        }

        // 🎯 BEAUTIFUL TRADE RESULTS DISPLAY
        fn display_trade_results(trade: &TradeRecord) {
            let expected_profit = trade.expected_profit;
            let actual_profit = trade.actual_profit.unwrap_or(Decimal::ZERO);
            let profit_delta = actual_profit - expected_profit;
            
            let expected_spread = trade.expected_spread_percent;
            let actual_spread = trade.actual_spread_percent.unwrap_or(Decimal::ZERO);
            let spread_delta = actual_spread - expected_spread;
            
            let buy_slippage = trade.buy_slippage_percent.unwrap_or(Decimal::ZERO);
            let sell_slippage = trade.sell_slippage_percent.unwrap_or(Decimal::ZERO);
            let total_slippage = trade.total_slippage_percent.unwrap_or(Decimal::ZERO);
            
            let execution_time = trade.execution_time_ms.unwrap_or(0);
            
            // 🎯 Beautiful trade summary
            log::info!("📊 PROFIT: Expected ${:.4} -> Actual ${:.4} (Δ: ${:+.4})", 
                expected_profit, actual_profit, profit_delta);
            log::info!("📈 SPREAD: Expected {:.4}% -> Actual {:.4}% (Δ: {:+.4}%)", 
                expected_spread, actual_spread, spread_delta);
            log::info!("🎯 SLIPPAGE: {:.3}% (Buy: {:.3}%, Sell: {:.3}%)", 
                total_slippage, buy_slippage, sell_slippage);
            log::info!("⏱️ EXECUTION: {}ms", execution_time);
            
            // Additional summary line for quick scanning
            log::info!("🚀 TRADE COMPLETE: {} | ${:.4} profit | {:.3}% slippage | {}ms", 
                trade.trade_id, actual_profit, total_slippage, execution_time);
        }

        // ✅ NEW: Enhanced risk status logging
        pub fn log_risk_status(state: &SharedState, sol_price: Decimal) {
            let current_capital = state.get_current_capital(sol_price);
            let trading_pnl = state.get_trading_pnl();
            let trading_pnl_percent = state.get_trading_pnl_percent();
            let max_trade_size = current_capital * (config::MAX_CAPITAL_PER_TRADE_PERCENT / dec!(100.0));
            let trading_stats = state.get_trading_stats();
            
            log::info!("[RISK] 📊 Current Status:");
            log::info!("[RISK] 💰 Capital: ${:.4} (Start: ${:.4})", current_capital, state.session_start_capital);
            log::info!("[RISK] 📈 Trading P&L: ${:+.4} ({:+.2}%) - {} trades", trading_pnl, trading_pnl_percent, trading_stats.total_trades);
            log::info!("[RISK] 🎯 Max Trade Size: ${:.4} ({}%)", max_trade_size, config::MAX_CAPITAL_PER_TRADE_PERCENT);
            log::info!("[RISK] 🛡️ Trading Drawdown Limit: {:.1}%", config::MAX_DAILY_DRAWDOWN_PERCENT);
            log::info!("[RISK] 📊 Success Rate: {:.1}% ({}/{} trades)", trading_stats.success_rate, trading_stats.successful_trades, trading_stats.total_trades);
            
            if trading_stats.total_trades > 0 {
                log::info!("[RISK] 🏆 Best Trade: ${:+.4} | Worst Trade: ${:+.4}", trading_stats.largest_win, trading_stats.largest_loss);
            }
        }
    }
}

// ---- Logging Module ----
mod logger {
    pub fn init() {
        dotenvy::dotenv().ok();
        if std::env::var("RUST_LOG").is_err() {
            unsafe {
                std::env::set_var("RUST_LOG", "info");
            }
        }
        env_logger::init();
    }
}

// ---- MEXC Connector ----
mod mexc {
    use crate::{config, state::{PriceUpdate, Price}};
    use futures_util::{StreamExt, SinkExt};
    use tokio::sync::mpsc::Sender;
    use serde_json::Value;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    pub async fn run_mexc_connector(
        is_running: Arc<AtomicBool>,
        price_sender: Sender<PriceUpdate>
    ) {
        log::info!("[MEXC] 🚀 Zero-Fee Connector starting for SOL/USDC - TRUE zero fees!");
        log::info!("[MEXC] 🎉 Connected to Zero-Fee Zone for {} - 0% maker/taker fees!", config::MEXC_TARGET_PAIR);
        
        while is_running.load(Ordering::SeqCst) {
            if let Err(e) = connect_and_run(is_running.clone(), &price_sender).await {
                log::error!("[MEXC] Connection error: {}. Retrying in 5 seconds...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
        log::info!("[MEXC] Connector shutting down.");
    }

    async fn connect_and_run(is_running: Arc<AtomicBool>, price_sender: &Sender<PriceUpdate>) -> anyhow::Result<()> {
        let (mut ws_stream, _) = tokio_tungstenite::connect_async(config::MEXC_WEBSOCKET_URL).await?;
        log::info!("[MEXC] WebSocket connection established to zero-fee exchange.");

        let subscribe_msg = serde_json::json!({
            "method": "SUBSCRIPTION",
            "params": [format!("spot@public.deals.v3.api@{}", config::MEXC_TARGET_PAIR)]
        });

        ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string())).await?;
        log::info!("[MEXC] 🎉 Subscribed to {} trade stream in Zero-Fee Zone!", config::MEXC_TARGET_PAIR);

        let mut last_ping = std::time::Instant::now();

        while is_running.load(Ordering::SeqCst) {
            tokio::select!{
                Some(msg) = ws_stream.next() => {
                    let msg = msg?;
                    if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                        // Skip system messages
                        if text.contains("SUBSCRIPTION") || text.contains("PONG") || text.contains("code") {
                            log::debug!("[MEXC] System message: {}", text);
                            continue;
                        }
                        
                        // 🔧 DEBUG: Log all non-system messages to see what we're receiving
                        log::debug!("[MEXC] Raw message: {}", text);
                        
                        if let Ok(json_data) = serde_json::from_str::<Value>(&text) {
                            log::debug!("[MEXC] Parsed JSON: {:?}", json_data);
                            
                            if let Some(data) = json_data.get("d") {
                                if let Some(deals) = data.get("deals").and_then(|d| d.as_array()) {
                                    for deal in deals {
                                        if let Some(price_str) = deal.get("p").and_then(|p| p.as_str()) {
                                            match price_str.parse::<Price>() {
                                                Ok(price) => {
                                                    log::info!("[MEXC] 🚀 SOL/USDC price: ${:.4}", price);
                                                    if price_sender.send(PriceUpdate::MEXC(price)).await.is_err() {
                                                        log::error!("[MEXC] Failed to send price to engine. Channel closed.");
                                                        break;
                                                    }
                                                }
                                                Err(e) => {
                                                    log::warn!("[MEXC] Could not parse price string '{}'. Error: {}", price_str, e);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    log::debug!("[MEXC] No deals array found in data: {:?}", data);
                                }
                            } else {
                                log::debug!("[MEXC] No 'd' field found in JSON: {:?}", json_data);
                            }
                        } else {
                            log::debug!("[MEXC] Failed to parse JSON: {}", text);
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if last_ping.elapsed() >= Duration::from_secs(30) {
                        let ping_msg = serde_json::json!({"method": "PING"});
                        if let Err(e) = ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(ping_msg.to_string())).await {
                            log::warn!("[MEXC] Failed to send ping: {}", e);
                            break;
                        }
                        last_ping = std::time::Instant::now();
                    }
                }
            }
        }
        Ok(())
    }
}

// ---- Solana Jupiter Connector ----
// ---- FIXED Solana Jupiter Connector with Pro API Headers ----
mod solana {
    use crate::{config, state::{Price, PriceUpdate}};
    use reqwest;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use rust_decimal::prelude::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct JupiterPriceResponse {
        // ✅ UPDATED: v3 response is direct object, no "data" wrapper
        #[serde(flatten)]
        prices: std::collections::HashMap<String, JupiterPrice>,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct JupiterPrice {
        #[serde(rename = "usdPrice")]  // ✅ UPDATED: "price" → "usdPrice"
        usd_price: f64,
        #[serde(rename = "blockId")]
        block_id: u64,
        decimals: u8,
        #[serde(rename = "priceChange24h")]
        price_change_24h: f64,
    }

    pub async fn run_jupiter_connector(
        is_running: Arc<AtomicBool>,
        price_sender: tokio::sync::mpsc::Sender<PriceUpdate>,
    ) {
        log::info!("[JUPITER] 🚀 Pro Connector starting for Solana DEX prices (3000 RPM on PAID tier).");
        
        let app_config = config::AppConfig::new();
        // 🔧 FIXED: Use x-api-key header
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-api-key", reqwest::header::HeaderValue::from_str(&app_config.jupiter_api_key).unwrap());
                headers.insert("Content-Type", reqwest::header::HeaderValue::from_str("application/json").unwrap());
                headers
            })
            .build()
            .expect("Failed to build Jupiter Pro client");

        log::info!("[JUPITER] ✅ Pro client initialized with PAID tier API");

        while is_running.load(Ordering::SeqCst) {
            match fetch_jupiter_price(&client, &app_config.jupiter_api_url).await {
                Ok(price) => {
                    log::info!("[JUPITER] 🚀 SOL price: ${:.4}", price);
                    if price_sender.send(PriceUpdate::Solana(price)).await.is_err() {
                        log::error!("[JUPITER] Failed to send price to engine. Channel closed.");
                        break;
                    }
                }
                Err(e) => {
                    log::warn!("[JUPITER] Pro API failed to fetch price: {}", e);
                    // 🆕 ADD: Check if it's a rate limit error specifically
                    if e.to_string().contains("429") || e.to_string().contains("rate limit") {
                        log::error!("[JUPITER] 🚨 RATE LIMITED on Pro plan! Check API key activation.");
                        log::error!("[JUPITER] 🚨 API Key being used: {}...{}", 
                            &app_config.jupiter_api_key[..8], 
                            &app_config.jupiter_api_key[app_config.jupiter_api_key.len()-4..]);
                    }
                }
            }
            
            // 🔧 ADJUSTED: Faster polling for Pro plan (600 RPM = 10 RPS = 0.1s intervals)
            tokio::time::sleep(Duration::from_millis(config::JUPITER_PRICE_INTERVAL_MS)).await;
        }
        log::info!("[JUPITER] Pro connector shutting down.");
    }

    async fn fetch_jupiter_price(client: &reqwest::Client, base_url: &str) -> Result<Price, Box<dyn std::error::Error + Send + Sync>> {
        // 🔧 FIXED: Remove token from URL, headers already have x-api-key
        let url = format!("{}?ids={}", base_url, config::SOL_MINT_ADDRESS);
        
        log::debug!("[JUPITER] 🚀 Fetching price from PAID tier API");
        
        // No need for API key in headers anymore
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send() // Headers already include x-api-key
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            
            if status.as_u16() == 429 {
                log::error!("[JUPITER] 🚨 Rate limited! Status: {}, Response: {}", status, error_text);
                return Err(format!("Rate limited ({}): {}", status, error_text).into());
            }
            
            return Err(format!("HTTP error ({}): {}", status, error_text).into());
        }

        let price_data: serde_json::Value = response.json().await?;
        
        if let Some(sol_price) = price_data[config::SOL_MINT_ADDRESS]["usdPrice"].as_f64() {
            let price_decimal = Price::from_f64(sol_price)
                .ok_or("Failed to convert Jupiter price to Decimal")?;
            
            if price_decimal < Price::from(10) || price_decimal > Price::from(1000) {
                return Err(format!("Jupiter price ${} seems unreasonable", price_decimal).into());
            }
            
            log::debug!("[JUPITER] ✅ Pro API v3 price parsed: ${:.4}", price_decimal);
            Ok(price_decimal)
        } else {
            Err("SOL price not found in Jupiter v3 response".into())
        }
    }
}

// ---- FIXED Arbitrage Engine Module ----
mod engine {
    use super::*;
    use crate::{config, risk::RiskManager, state::{PriceUpdate, SharedState, PriceInfo, PriceDataPoint}};
    use crate::{mexc_trading::MexcTradingClient, solana_trading::SolanaTradingClient};
    use tokio::sync::mpsc::Receiver;
    use rust_decimal_macros::dec;
    use chrono::Utc;

    pub async fn run_coordinated_arbitrage_engine(
        is_running: Arc<AtomicBool>,
        state: Arc<Mutex<SharedState>>,
        mut price_receiver: Receiver<PriceUpdate>,
        coordinator: TradingCoordinator,
    ) {
        log::info!("[ENGINE] 🚀 Coordinated Arbitrage Engine started!");
        
        // Initialize trading clients for real trading
        let (mexc_client, solana_client) = if !config::PAPER_TRADING {
            log::info!("[ENGINE] 🚨 REAL TRADING MODE - Initializing trading clients...");
            
            let app_config = config::AppConfig::new();
            
            let mexc_client = Some(MexcTradingClient::new(
                app_config.mexc_api_key.clone(), 
                app_config.mexc_api_secret.clone()
            ));
            
            let solana_client = match SolanaTradingClient::new(
                app_config.solana_rpc_url.clone(),
                app_config.solana_wallet_path.clone(),
                app_config.jupiter_api_key.clone()
            ) {
                Ok(client) => {
                    log::info!("[ENGINE] ✅ Solana client initialized. Wallet: {}", client.get_wallet_address());
                    Some(client)
                },
                Err(e) => {
                    log::error!("[ENGINE] ❌ Failed to initialize Solana client: {}", e);
                    None
                }
            };
            
            (mexc_client, solana_client)
        } else {
            log::info!("[ENGINE] 📝 Paper trading mode - no real clients needed");
            (None, None)
        };

        while is_running.load(Ordering::SeqCst) {
            if let Some(update) = price_receiver.recv().await {
                
                // Update prices
                let (mexc_price, solana_price) = {
                    let mut state_guard = state.lock().unwrap();
                    
                    let (price, source) = match update {
                        PriceUpdate::MEXC(p) => (p, "MEXC"),
                        PriceUpdate::Solana(p) => (p, "Jupiter"),
                    };
                    
                    state_guard.price_history.push(PriceDataPoint {
                        timestamp: Utc::now(),
                        source: source.to_string(),
                        price,
                    });

                    let price_info = PriceInfo {
                        price,
                        timestamp: Utc::now(),
                    };
                    
                    match source {
                        "MEXC" => state_guard.mexc_price = Some(price_info),
                        "Jupiter" => state_guard.solana_price = Some(price_info),
                        _ => {}
                    }

                    (state_guard.mexc_price, state_guard.solana_price)
                };

                // ✅ CHECK: Skip trading if rebalancing is in progress
                if !coordinator.can_start_trade() {
                    log::debug!("[ENGINE] ⏸️ Skipping arbitrage check - rebalancing in progress");
                    continue;
                }

                // Check for arbitrage opportunities
                if let (Some(mexc), Some(solana)) = (mexc_price, solana_price) {
                    let p_mexc = mexc.price;
                    let p_solana = solana.price;

                    if p_mexc.is_zero() || p_solana.is_zero() {
                        continue; 
                    }

                    let spread1 = (p_solana - p_mexc) / p_mexc * dec!(100.0);
                    let spread2 = (p_mexc - p_solana) / p_solana * dec!(100.0);

                    // Opportunity 1: Buy MEXC, Sell Solana
                    if spread1 > dec!(0.0) {
                        let trade_value = config::TRADE_AMOUNT_SOL * p_mexc;
                        let net_spread = spread1 - ((config::SOLANA_GAS_FEE_USD / trade_value) * dec!(100.0));
                        
                        if net_spread > config::MIN_PROFIT_SPREAD_PERCENT {
                            let should_trade = {
                                let state_guard = state.lock().unwrap();
                                RiskManager::check_trade(&state_guard, trade_value, p_mexc) &&
                                RiskManager::check_price_freshness(&state_guard)
                            };
                            
                            if should_trade {
                                // ✅ EXECUTE COORDINATED TRADE
                                execute_coordinated_trade(
                                    &mexc_client,
                                    &solana_client,
                                    &coordinator,
                                    &state,
                                    "MEXC",
                                    config::TRADE_AMOUNT_SOL,
                                    p_mexc,
                                    p_solana,
                                    net_spread
                                ).await;
                            }
                        }
                    }

                    // Opportunity 2: Buy Solana, Sell MEXC
                    if spread2 > dec!(0.0) {
                        let trade_value = config::TRADE_AMOUNT_SOL * p_solana;
                        let net_spread = spread2 - ((config::SOLANA_GAS_FEE_USD / trade_value) * dec!(100.0));
                        
                        if net_spread > config::MIN_PROFIT_SPREAD_PERCENT {
                            let should_trade = {
                                let state_guard = state.lock().unwrap();
                                RiskManager::check_trade(&state_guard, trade_value, p_mexc) &&
                                RiskManager::check_price_freshness(&state_guard)
                            };
                            
                            if should_trade {
                                // ✅ EXECUTE COORDINATED TRADE
                                execute_coordinated_trade(
                                    &mexc_client,
                                    &solana_client,
                                    &coordinator,
                                    &state,
                                    "SOLANA",
                                    config::TRADE_AMOUNT_SOL,
                                    p_solana,
                                    p_mexc,
                                    net_spread
                                ).await;
                            }
                        }
                    }
                }
            } else {
                is_running.store(false, Ordering::SeqCst);
            }
        }
        
        log::info!("[ENGINE] 🚀 Coordinated engine shutting down.");
    }

    async fn execute_coordinated_trade(
        mexc_client: &Option<MexcTradingClient>,
        solana_client: &Option<SolanaTradingClient>,
        coordinator: &TradingCoordinator,
        state: &Arc<Mutex<SharedState>>,
        venue_bought: &str,
        amount_sol: Decimal,
        buy_price: Decimal,
        sell_price: Decimal,
        net_spread: Decimal,
    ) {
        let trade_id = format!("TRADE_{}", Utc::now().format("%Y%m%d_%H%M%S_%3f"));
        
        // ✅ REGISTER TRADE with coordinator
        if !coordinator.register_trade(trade_id.clone()) {
            log::warn!("[ENGINE] ⚠️ Cannot start trade {} - rebalancing in progress", trade_id);
            return;
        }
        
        // Ensure trade is unregistered when done
        let cleanup_coordinator = coordinator.clone();
        let cleanup_trade_id = trade_id.clone();
        let _guard = scopeguard::guard((), move |_| {
            cleanup_coordinator.unregister_trade(&cleanup_trade_id);
        });
        
        log::info!("[ENGINE] 🚀 COORDINATED TRADE: {} - Buy {} @{:.4}, Sell {} @{:.4}", 
            trade_id, venue_bought, buy_price, 
            if venue_bought == "MEXC" { "Solana" } else { "MEXC" }, sell_price);
        log::info!("[ENGINE] 📈 Net Spread: {:.4}%", net_spread);
        
        if config::PAPER_TRADING {
            // Paper trading
            let mut state_guard = state.lock().unwrap();
            RiskManager::update_capital(&mut state_guard, venue_bought, amount_sol, buy_price, sell_price, buy_price);
        } else {
            // Real trading
            if let (Some(ref mexc), Some(ref solana)) = (mexc_client, solana_client) {
                match RiskManager::execute_parallel_arbitrage(
                    mexc,
                    solana,
                    venue_bought,
                    amount_sol,
                    buy_price,
                    sell_price
                ).await {
                    Ok(trade_record) => {
                        log::info!("[ENGINE] ✅ Coordinated trade {} completed! Profit: ${:.4}", 
                            trade_id, trade_record.actual_profit.unwrap_or(Decimal::ZERO));
                        
                        let mut state_guard = state.lock().unwrap();
                        state_guard.add_trade_record(trade_record);
                    },
                    Err(e) => {
                        log::error!("[ENGINE] ❌ Coordinated trade {} failed: {}", trade_id, e);
                    }
                }
            } else {
                log::error!("[ENGINE] ❌ Trading clients not available for {}", trade_id);
            }
        }
        
        // Trade cleanup happens automatically via _guard
    }
}
// 🚀 NEW: Automatic Balance Fetcher Module

mod balance_fetcher {
    use crate::{config, mexc_trading::MexcTradingClient, solana_trading::SolanaTradingClient};
    use rust_decimal::Decimal; 
    use rust_decimal_macros::dec;

    #[derive(Debug, Clone)]
    pub struct LiveBalances {
        pub mexc_usdc: Decimal,
        pub mexc_sol: Decimal,
        pub wallet_usdc: Decimal,
        pub wallet_sol: Decimal,
    }

    impl LiveBalances {
        pub fn total_capital_usd(&self, sol_price: Decimal) -> Decimal {
            self.mexc_usdc + 
            (self.mexc_sol * sol_price) +
            self.wallet_usdc + 
            (self.wallet_sol * sol_price)
        }
    }

    pub struct BalanceFetcher;

    impl BalanceFetcher {
        /// 🚀 Main function: Fetch all account balances before starting trading
        pub async fn fetch_all_balances() -> Result<LiveBalances, Box<dyn std::error::Error + Send + Sync>> {
            log::info!("[BALANCE] 🔍 Fetching live account balances...");
            
            let app_config = config::AppConfig::new();
            
            // Initialize clients
            let mexc_client = MexcTradingClient::new(
                app_config.mexc_api_key.clone(),
                app_config.mexc_api_secret.clone()
            );
            
            // 🔧 CRITICAL FIX: Pass jupiter_api_key instead of jupiter_api_url
            let solana_client = SolanaTradingClient::new(
                app_config.solana_rpc_url.clone(),
                app_config.solana_wallet_path.clone(),
                app_config.jupiter_api_key.clone()  // ✅ FIXED: Was jupiter_api_url, now jupiter_api_key
            )?;

            // Log which API key we're using for verification
            log::info!("[BALANCE] 🔑 Using Jupiter Pro API key: {}...{}", 
                &app_config.jupiter_api_key[..8], 
                &app_config.jupiter_api_key[app_config.jupiter_api_key.len()-4..]);

            // Fetch balances in parallel for speed
            log::info!("[BALANCE] 📊 Fetching MEXC and Solana balances in parallel...");
            
            let mexc_future = Self::fetch_mexc_balances(&mexc_client);
            let solana_future = Self::fetch_solana_balances(&solana_client);
            
            let (mexc_result, solana_result) = tokio::join!(mexc_future, solana_future);
            
            // Process results
            let (mexc_usdc, mexc_sol) = mexc_result?;
            let (wallet_usdc, wallet_sol) = solana_result?;
            
            let balances = LiveBalances {
                mexc_usdc,
                mexc_sol,
                wallet_usdc,
                wallet_sol,
            };
            
            // Enhanced balance summary
            log::info!("[BALANCE] ✅ Live balances fetched successfully!");
            log::info!("[BALANCE] 💰 MEXC USDC: ${:.4}", balances.mexc_usdc);
            log::info!("[BALANCE] 🪙 MEXC SOL: {:.6} SOL", balances.mexc_sol);
            log::info!("[BALANCE] 🏦 Wallet USDC: ${:.4}", balances.wallet_usdc);
            log::info!("[BALANCE] 🪙 Wallet SOL: {:.6} SOL", balances.wallet_sol);
            
            Ok(balances)
        }

        /// Fetch both MEXC USDC and SOL balances
        async fn fetch_mexc_balances(
            mexc_client: &MexcTradingClient
        ) -> Result<(Decimal, Decimal), Box<dyn std::error::Error + Send + Sync>> {
            
            log::debug!("[BALANCE] 🔍 Fetching MEXC account balances (USDC + SOL)...");
            
            let account_data = mexc_client.get_account_balance().await?;
            
            let mut mexc_usdc = Decimal::ZERO;
            let mut mexc_sol = Decimal::ZERO;
            
            // Parse MEXC response to find both USDC and SOL balances
            if let Some(balances) = account_data.get("balances").and_then(|b| b.as_array()) {
                for balance in balances {
                    if let Some(asset) = balance.get("asset").and_then(|a| a.as_str()) {
                        if let Some(free_str) = balance.get("free").and_then(|f| f.as_str()) {
                            let free_balance = free_str.parse::<Decimal>()
                                .map_err(|_| format!("Failed to parse MEXC {} balance", asset))?;
                            
                            match asset {
                                "USDC" => {
                                    mexc_usdc = free_balance;
                                    log::debug!("[BALANCE] ✅ MEXC USDC balance: ${:.4}", free_balance);
                                },
                                "SOL" => {
                                    mexc_sol = free_balance;
                                    log::debug!("[BALANCE] ✅ MEXC SOL balance: {:.6} SOL", free_balance);
                                },
                                _ => {} // Skip other assets
                            }
                        }
                    }
                }
            }
            
            log::debug!("[BALANCE] 📊 MEXC Totals - USDC: ${:.4}, SOL: {:.6}", mexc_usdc, mexc_sol);
            Ok((mexc_usdc, mexc_sol))
        }

        /// Fetch Solana wallet balances (USDC + SOL)
        async fn fetch_solana_balances(
            solana_client: &SolanaTradingClient
        ) -> Result<(Decimal, Decimal), Box<dyn std::error::Error + Send + Sync>> {
            
            log::debug!("[BALANCE] 🔍 Fetching Solana wallet balances...");
            
            let wallet_address = solana_client.get_wallet_address();
            
            // Fetch both SOL and USDC balances in parallel
            let sol_future = Self::fetch_sol_balance(solana_client, &wallet_address);
            let usdc_future = Self::fetch_usdc_balance(solana_client, &wallet_address);
            
            let (sol_result, usdc_result) = tokio::join!(sol_future, usdc_future);
            
            let sol_balance = sol_result?;
            let usdc_balance = usdc_result?;
            
            log::debug!("[BALANCE] ✅ Solana SOL: {:.6}, USDC: ${:.4}", sol_balance, usdc_balance);
            Ok((usdc_balance, sol_balance))
        }

        /// Fetch SOL balance using RPC
        async fn fetch_sol_balance(
            solana_client: &SolanaTradingClient,
            wallet_address: &str
        ) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            
            // Use the client's internal RPC to get SOL balance
            let pubkey = wallet_address.parse::<solana_sdk::pubkey::Pubkey>()
                .map_err(|_| "Invalid wallet address")?;
            
            let balance_lamports = solana_client.get_sol_balance(&pubkey).await?;
            
            // Convert lamports to SOL (1 SOL = 1,000,000,000 lamports)
            let sol_balance = Decimal::from(balance_lamports) / dec!(1_000_000_000);
            
            Ok(sol_balance)
        }

        /// Fetch USDC balance using the client's method
        async fn fetch_usdc_balance(
            solana_client: &SolanaTradingClient,
            wallet_address: &str
        ) -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
            
            // Use the client's get_usdc_balance method (now with correct API key)
            let usdc_balance = solana_client.get_usdc_balance(wallet_address).await?;
            
            Ok(usdc_balance)
        }
    }
}
// ✅ NEW: Dedicated Auto-Rebalancing Module  
// ✅ FIXED Auto-Rebalancing Module  
mod auto_rebalancer {
    use super::*;
    use crate::{config, state::SharedState, mexc_trading::MexcTradingClient, solana_trading::SolanaTradingClient, balance_fetcher};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    pub async fn run_coordinated_rebalance_monitor(
        is_running: Arc<AtomicBool>,
        state: Arc<Mutex<SharedState>>,
        coordinator: TradingCoordinator,
    ) {
        log::info!("[REBALANCE] 🔄 Coordinated auto-rebalancing monitor started");
        log::info!("[REBALANCE] ⚙️ Config: {}% trigger, {}% target, ${:.0} min value", 
            config::REBALANCE_THRESHOLD_PERCENT, config::REBALANCE_TARGET_PERCENT, config::REBALANCE_MIN_VALUE_USD);
        
        // Initialize trading clients
        let app_config = config::AppConfig::new();
        
        let mexc_client = MexcTradingClient::new(
            app_config.mexc_api_key.clone(),
            app_config.mexc_api_secret.clone()
        );
        
        let solana_client = match SolanaTradingClient::new(
            app_config.solana_rpc_url.clone(),
            app_config.solana_wallet_path.clone(),
            app_config.jupiter_api_key.clone()
        ) {
            Ok(client) => {
                log::info!("[REBALANCE] ✅ Solana client initialized for coordinated rebalancing");
                Some(client)
            },
            Err(e) => {
                log::error!("[REBALANCE] ❌ Failed to initialize Solana client: {}", e);
                None
            }
        };
        
        while is_running.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(config::REBALANCE_CHECK_INTERVAL_SECONDS)).await;
            
            if !is_running.load(Ordering::SeqCst) {
                break;
            }
            
            // ✅ COORDINATED REBALANCING CHECK
            if needs_rebalancing(&state).await {
                log::info!("[REBALANCE] 🔄 Rebalancing needed - requesting coordination...");
                
                // Request coordinated rebalancing
                if coordinator.start_rebalancing(45).await { // 45 second timeout
                    // Perform rebalancing
                    let result = perform_coordinated_rebalancing(&mexc_client, &solana_client).await;
                    
                    // Always finish rebalancing
                    coordinator.finish_rebalancing();
                    
                    match result {
                        Ok(_) => log::info!("[REBALANCE] ✅ Coordinated rebalancing completed successfully"),
                        Err(e) => log::error!("[REBALANCE] ❌ Coordinated rebalancing failed: {}", e),
                    }
                } else {
                    log::warn!("[REBALANCE] ⚠️ Could not start rebalancing - trades did not complete in time");
                }
            } else {
                log::debug!("[REBALANCE] ✅ No rebalancing needed");
            }
        }
        
        log::info!("[REBALANCE] 🔄 Coordinated rebalancing monitor shutting down");
    }

    async fn needs_rebalancing(_state: &Arc<Mutex<SharedState>>) -> bool {
        // Get current SOL price
        let sol_price = match get_current_sol_price().await {
            Ok(price) => price,
            Err(_) => return false,
        };
        
        // Fetch current balances
        let balances = match balance_fetcher::BalanceFetcher::fetch_all_balances().await {
            Ok(b) => b,
            Err(_) => return false,
        };
        
        // Check if rebalancing is needed
        let mexc_total = balances.mexc_usdc + (balances.mexc_sol * sol_price);
        let wallet_total = balances.wallet_usdc + (balances.wallet_sol * sol_price);
        
        if mexc_total >= config::REBALANCE_MIN_VALUE_USD {
            let mexc_usdc_percent = (balances.mexc_usdc / mexc_total) * dec!(100.0);
            let mexc_sol_percent = ((balances.mexc_sol * sol_price) / mexc_total) * dec!(100.0);
            
            if mexc_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT || 
               mexc_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
                return true;
            }
        }
        
        if wallet_total >= config::REBALANCE_MIN_VALUE_USD {
            let wallet_usdc_percent = (balances.wallet_usdc / wallet_total) * dec!(100.0);
            let wallet_sol_percent = ((balances.wallet_sol * sol_price) / wallet_total) * dec!(100.0);
            
            if wallet_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT || 
               wallet_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
                return true;
            }
        }
        
        false
    }

    async fn perform_coordinated_rebalancing(
        mexc_client: &MexcTradingClient,
        solana_client: &Option<SolanaTradingClient>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        
        log::info!("[REBALANCE] 🔄 Performing coordinated rebalancing...");
        
        // Get current state
        let sol_price = get_current_sol_price().await?;
        let balances = balance_fetcher::BalanceFetcher::fetch_all_balances().await?;
        
        // Rebalance MEXC
        rebalance_mexc_coordinated(mexc_client, &balances, sol_price).await?;
        
        // Rebalance Wallet
        if let Some(solana) = solana_client {
            rebalance_wallet_coordinated(solana, &balances, sol_price).await?;
        }
        
        Ok(())
    }

    async fn rebalance_mexc_coordinated(
        mexc_client: &MexcTradingClient,
        balances: &balance_fetcher::LiveBalances,
        sol_price: Decimal,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        
        let mexc_usdc_value = balances.mexc_usdc;
        let mexc_sol_value = balances.mexc_sol * sol_price;
        let mexc_total = mexc_usdc_value + mexc_sol_value;
        
        if mexc_total < config::REBALANCE_MIN_VALUE_USD {
            return Ok(());
        }
        
        let mexc_usdc_percent = (mexc_usdc_value / mexc_total) * dec!(100.0);
        let mexc_sol_percent = (mexc_sol_value / mexc_total) * dec!(100.0);
        
        log::info!("[REBALANCE] 📊 MEXC Current: {:.1}% USDC / {:.1}% SOL", mexc_usdc_percent, mexc_sol_percent);
        
        if mexc_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT {
            let target_usdc_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
            let usdc_to_convert = mexc_usdc_value - target_usdc_value;
            
            if usdc_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                let sol_to_buy = usdc_to_convert / sol_price;
                
                log::info!("[REBALANCE] 🔄 MEXC: Converting ${:.2} USDC to {:.4} SOL", usdc_to_convert, sol_to_buy);
                
                mexc_client.place_order_with_current_price("SOLUSDC", "BUY", "MARKET", sol_to_buy, None).await?;
                log::info!("[REBALANCE] ✅ MEXC rebalance completed");
            }
        } else if mexc_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
            let target_sol_value = mexc_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
            let sol_value_to_convert = mexc_sol_value - target_sol_value;
            
            if sol_value_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                let sol_to_sell = sol_value_to_convert / sol_price;
                
                log::info!("[REBALANCE] 🔄 MEXC: Converting {:.4} SOL to ${:.2} USDC", sol_to_sell, sol_value_to_convert);
                
                mexc_client.place_order_with_current_price("SOLUSDC", "SELL", "MARKET", sol_to_sell, None).await?;
                log::info!("[REBALANCE] ✅ MEXC rebalance completed");
            }
        }
        
        Ok(())
    }

    async fn rebalance_wallet_coordinated(
        solana_client: &SolanaTradingClient,
        balances: &balance_fetcher::LiveBalances,
        sol_price: Decimal,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        
        let wallet_usdc_value = balances.wallet_usdc;
        let wallet_sol_value = balances.wallet_sol * sol_price;
        let wallet_total = wallet_usdc_value + wallet_sol_value;
        
        if wallet_total < config::REBALANCE_MIN_VALUE_USD {
            return Ok(());
        }
        
        let wallet_usdc_percent = (wallet_usdc_value / wallet_total) * dec!(100.0);
        let wallet_sol_percent = (wallet_sol_value / wallet_total) * dec!(100.0);
        
        log::info!("[REBALANCE] 📊 Wallet Current: {:.1}% USDC / {:.1}% SOL", wallet_usdc_percent, wallet_sol_percent);
        
        if wallet_usdc_percent >= config::REBALANCE_THRESHOLD_PERCENT {
            let target_usdc_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
            let usdc_to_convert = wallet_usdc_value - target_usdc_value;
            
            if usdc_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                let usdc_lamports = (usdc_to_convert * dec!(1_000_000)).to_u64().unwrap_or(0);
                
                log::info!("[REBALANCE] 🔄 Wallet: Converting ${:.2} USDC to SOL", usdc_to_convert);
                
                let result = solana_client.execute_swap_with_confirmation(
                    config::USDC_MINT_ADDRESS,
                    config::SOL_MINT_ADDRESS,
                    usdc_lamports,
                    50,
                ).await?;
                
                if result.confirmed {
                    log::info!("[REBALANCE] ✅ Wallet rebalance completed in {}ms", result.confirmation_time_ms);
                }
            }
        } else if wallet_sol_percent >= config::REBALANCE_THRESHOLD_PERCENT {
            let target_sol_value = wallet_total * (config::REBALANCE_TARGET_PERCENT / dec!(100.0));
            let sol_value_to_convert = wallet_sol_value - target_sol_value;
            
            if sol_value_to_convert >= config::REBALANCE_MIN_TRADE_USD {
                let sol_to_convert = sol_value_to_convert / sol_price;
                let sol_lamports = (sol_to_convert * dec!(1_000_000_000)).to_u64().unwrap_or(0);
                
                log::info!("[REBALANCE] 🔄 Wallet: Converting {:.4} SOL to USDC", sol_to_convert);
                
                let result = solana_client.execute_swap_with_confirmation(
                    config::SOL_MINT_ADDRESS,
                    config::USDC_MINT_ADDRESS,
                    sol_lamports,
                    50,
                ).await?;
                
                if result.confirmed {
                    log::info!("[REBALANCE] ✅ Wallet rebalance completed in {}ms", result.confirmation_time_ms);
                }
            }
        }
        
        Ok(())
    }

    async fn get_current_sol_price() -> Result<Decimal, Box<dyn std::error::Error + Send + Sync>> {
        let app_config = config::AppConfig::new();
        
        let client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-api-key", reqwest::header::HeaderValue::from_str(&app_config.jupiter_api_key).unwrap());
                headers
            })
            .build()?;
        
        let url = format!("{}?ids={}", app_config.jupiter_api_url, config::SOL_MINT_ADDRESS);
        
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let price_data: serde_json::Value = response.json().await?;
        
        if let Some(sol_price) = price_data[config::SOL_MINT_ADDRESS]["usdPrice"].as_f64() {
            let price_decimal = Decimal::from_f64(sol_price)
                .ok_or("Failed to convert Jupiter price to Decimal")?;
            
            return Ok(price_decimal);
        }
        
        Err("SOL price not found in Jupiter v3 response".into())
    }
}
// ---- CSV Writer Module ----
mod csv_writer {
    use crate::state::PriceDataPoint;
    use chrono::Utc;
    use csv::WriterBuilder;

    pub fn write_to_csv(records: &[PriceDataPoint]) -> anyhow::Result<()> {
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
        let filename = format!("Data/Prices/mexc_arbitrage_log_{}.csv", timestamp);
        
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_path(&filename)?;

        writer.write_record(&["timestamp", "source", "price"])?;
        
        for record in records {
            writer.serialize(record)?;
        }

        writer.flush()?;
        log::info!("[CSV] 🚀 MEXC Zero-Fee price history saved to: {}", filename);
        Ok(())
    }
}
mod trade_csv_writer {
    use crate::state::TradeRecord;
    use crate::balance_fetcher::LiveBalances;
    use chrono::Utc;
    use csv::WriterBuilder;
    use rust_decimal::Decimal;

    pub fn write_trades_to_csv(trades: &[TradeRecord]) -> anyhow::Result<()> {
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
        let filename = format!("Data/Trades/mexc_arbitrage_trades_{}.csv", timestamp);
        
        let mut writer = WriterBuilder::new()
            .has_headers(true)
            .from_path(&filename)?;

        // Enhanced CSV header with detailed slippage tracking
        writer.write_record(&[
            "trade_id", "timestamp", "direction", "amount_sol", "buy_venue", "sell_venue",
            "expected_buy_price", "expected_sell_price", "actual_buy_price", "actual_sell_price",
            "expected_profit", "actual_profit", "profit_delta",
            "expected_spread_percent", "actual_spread_percent", "spread_delta",
            "buy_slippage_percent", "sell_slippage_percent", "total_slippage_percent",
            "buy_order_id", "sell_order_id", "solana_tx_signature", "total_fees",
            "execution_time_ms", "status"
        ])?;
        
        // Write enhanced trade records
        for trade in trades {
            let profit_delta = trade.actual_profit.unwrap_or(Decimal::ZERO) - trade.expected_profit;
            let spread_delta = trade.actual_spread_percent.unwrap_or(Decimal::ZERO) - trade.expected_spread_percent;
            
            let row = vec![
                trade.trade_id.clone(),
                trade.timestamp.to_rfc3339(),
                trade.direction.clone(),
                trade.amount_sol.to_string(),
                trade.buy_venue.clone(),
                trade.sell_venue.clone(),
                trade.buy_price.to_string(),
                trade.sell_price.to_string(),
                trade.actual_buy_price.map_or("".to_string(), |p| p.to_string()),
                trade.actual_sell_price.map_or("".to_string(), |p| p.to_string()),
                trade.expected_profit.to_string(),
                trade.actual_profit.map_or("".to_string(), |p| p.to_string()),
                profit_delta.to_string(),
                trade.expected_spread_percent.to_string(),
                trade.actual_spread_percent.map_or("".to_string(), |s| s.to_string()),
                spread_delta.to_string(),
                trade.buy_slippage_percent.map_or("".to_string(), |s| s.to_string()),
                trade.sell_slippage_percent.map_or("".to_string(), |s| s.to_string()),
                trade.total_slippage_percent.map_or("".to_string(), |s| s.to_string()),
                trade.buy_order_id.as_ref().map_or("".to_string(), |id| id.clone()),
                trade.sell_order_id.as_ref().map_or("".to_string(), |id| id.clone()),
                trade.solana_tx_signature.as_ref().map_or("".to_string(), |sig| sig.clone()),
                trade.total_fees.to_string(),
                trade.execution_time_ms.map_or("".to_string(), |t| t.to_string()),
                trade.status.clone(),
            ];
            
            writer.write_record(&row)?;
        }

        writer.flush()?;
        log::info!("[CSV] 🚀 Enhanced trade history saved to: {}", filename);
        Ok(())
    }

    // ✅ ENHANCED: Write comprehensive summary with starting and ending balances
    pub fn write_enhanced_trade_summary(
        trades: &[TradeRecord], 
        total_profit: Decimal, 
        trade_count: u32,
        initial_balances: &LiveBalances,
        final_balances: &LiveBalances,
        sol_price: Decimal,
        session_duration_minutes: i64
    ) -> anyhow::Result<()> {
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
        let filename = format!("Data/Summary/mexc_arbitrage_summary_{}.txt", timestamp);
        
        let successful_trades: Vec<_> = trades.iter().filter(|t| t.status == "SUCCESS").collect();
        let failed_trades = trades.len() - successful_trades.len();
        
        let avg_profit = if !successful_trades.is_empty() {
            successful_trades.iter()
                .filter_map(|t| t.actual_profit)
                .sum::<Decimal>() / Decimal::from(successful_trades.len())
        } else {
            Decimal::ZERO
        };
        
        let avg_slippage = if !successful_trades.is_empty() {
            successful_trades.iter()
                .filter_map(|t| t.total_slippage_percent)
                .sum::<Decimal>() / Decimal::from(successful_trades.len())
        } else {
            Decimal::ZERO
        };

        // Calculate total capital changes
        let initial_capital = initial_balances.total_capital_usd(sol_price);
        let final_capital = final_balances.total_capital_usd(sol_price);
        let total_capital_change = final_capital - initial_capital;
        let capital_change_percent = if initial_capital > Decimal::ZERO {
            (total_capital_change / initial_capital) * Decimal::from(100)
        } else {
            Decimal::ZERO
        };

        // Calculate balance changes
        let mexc_usdc_change = final_balances.mexc_usdc - initial_balances.mexc_usdc;
        let mexc_sol_change = final_balances.mexc_sol - initial_balances.mexc_sol;
        let wallet_usdc_change = final_balances.wallet_usdc - initial_balances.wallet_usdc;
        let wallet_sol_change = final_balances.wallet_sol - initial_balances.wallet_sol;

        let summary = format!(
r#"
MEXC ARBITRAGE BOT TRADING SESSION SUMMARY
==========================================
Session Timestamp: {}
Session Duration: {} minutes
SOL Price: ${:.4}

TRADING PERFORMANCE
===================
Total Trades Attempted: {}
Successful Trades: {}
Failed Trades: {}
Success Rate: {:.2}%
Total Trading Profit: ${:.4}
Average Profit per Trade: ${:.4}
Average Slippage: {:.4}%

CAPITAL SUMMARY
===============
Starting Total Capital: ${:.4}
Final Total Capital: ${:.4}
Total Capital Change: ${:+.4} ({:+.2}%)

MEXC EXCHANGE BALANCES
======================
MEXC USDC:
  Starting: ${:.4}
  Final:    ${:.4}
  Change:   ${:+.4}

MEXC SOL:
  Starting: {:.6} SOL
  Final:    {:.6} SOL  
  Change:   {:+.6} SOL

SOLANA WALLET BALANCES
======================
Wallet USDC:
  Starting: ${:.4}
  Final:    ${:.4}
  Change:   ${:+.4}

Wallet SOL:
  Starting: {:.6} SOL
  Final:    {:.6} SOL
  Change:   {:+.6} SOL

DETAILED BREAKDOWN
==================
Starting MEXC Value: ${:.4} (${:.4} USDC + {:.6} SOL @ ${:.4})
Final MEXC Value: ${:.4} (${:.4} USDC + {:.6} SOL @ ${:.4})
MEXC Value Change: ${:+.4}

Starting Wallet Value: ${:.4} (${:.4} USDC + {:.6} SOL @ ${:.4})
Final Wallet Value: ${:.4} (${:.4} USDC + {:.6} SOL @ ${:.4})
Wallet Value Change: ${:+.4}

RISK METRICS
============
Maximum Single Trade: 1.0 SOL (${:.4} at current price)
Daily Drawdown Limit: 20%
Actual Drawdown: {:.2}%
==========================================
Generated: {}
"#,
            timestamp,
            session_duration_minutes,
            sol_price,
            
            // Trading Performance
            trade_count,
            successful_trades.len(),
            failed_trades,
            if trade_count > 0 { (successful_trades.len() as f64 / trade_count as f64) * 100.0 } else { 0.0 },
            total_profit,
            avg_profit,
            avg_slippage,
            
            // Capital Summary
            initial_capital,
            final_capital,
            total_capital_change,
            capital_change_percent,
            
            // MEXC Balances
            initial_balances.mexc_usdc,
            final_balances.mexc_usdc,
            mexc_usdc_change,
            initial_balances.mexc_sol,
            final_balances.mexc_sol,
            mexc_sol_change,
            
            // Wallet Balances
            initial_balances.wallet_usdc,
            final_balances.wallet_usdc,
            wallet_usdc_change,
            initial_balances.wallet_sol,
            final_balances.wallet_sol,
            wallet_sol_change,
            
            // Detailed Breakdown
            initial_balances.mexc_usdc + (initial_balances.mexc_sol * sol_price),
            initial_balances.mexc_usdc, initial_balances.mexc_sol, sol_price,
            final_balances.mexc_usdc + (final_balances.mexc_sol * sol_price),
            final_balances.mexc_usdc, final_balances.mexc_sol, sol_price,
            (final_balances.mexc_usdc + (final_balances.mexc_sol * sol_price)) - (initial_balances.mexc_usdc + (initial_balances.mexc_sol * sol_price)),
            
            initial_balances.wallet_usdc + (initial_balances.wallet_sol * sol_price),
            initial_balances.wallet_usdc, initial_balances.wallet_sol, sol_price,
            final_balances.wallet_usdc + (final_balances.wallet_sol * sol_price),
            final_balances.wallet_usdc, final_balances.wallet_sol, sol_price,
            (final_balances.wallet_usdc + (final_balances.wallet_sol * sol_price)) - (initial_balances.wallet_usdc + (initial_balances.wallet_sol * sol_price)),
            
            // Risk Metrics
            sol_price,
            capital_change_percent.abs(),
            
            Utc::now().to_rfc3339()
        );

        std::fs::write(&filename, summary)?;
        log::info!("[SUMMARY] 📊 Enhanced trading summary saved to: {}", filename);
        Ok(())
    }

    // Keep the original function for backward compatibility
    pub fn write_trade_summary(trades: &[TradeRecord], total_profit: Decimal, trade_count: u32) -> anyhow::Result<()> {
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S");
        let filename = format!("Data/Summary/mexc_arbitrage_summary_{}.txt", timestamp);
        
        let successful_trades: Vec<_> = trades.iter().filter(|t| t.status == "SUCCESS").collect();
        let avg_profit = if !successful_trades.is_empty() {
            successful_trades.iter()
                .filter_map(|t| t.actual_profit)
                .sum::<Decimal>() / Decimal::from(successful_trades.len())
        } else {
            Decimal::ZERO
        };
        
        let avg_slippage = if !successful_trades.is_empty() {
            successful_trades.iter()
                .filter_map(|t| t.total_slippage_percent)
                .sum::<Decimal>() / Decimal::from(successful_trades.len())
        } else {
            Decimal::ZERO
        };

        let summary = format!(
            "MEXC ARBITRAGE BOT TRADING SUMMARY\n\
            =====================================\n\
            Session Timestamp: {}\n\
            Total Trades: {}\n\
            Successful Trades: {}\n\
            Total Profit: ${:.4}\n\
            Average Profit per Trade: ${:.4}\n\
            Average Slippage: {:.4}%\n\
            Success Rate: {:.2}%\n\
            =====================================\n",
            timestamp,
            trade_count,
            successful_trades.len(),
            total_profit,
            avg_profit,
            avg_slippage,
            if trade_count > 0 { (successful_trades.len() as f64 / trade_count as f64) * 100.0 } else { 0.0 }
        );

        std::fs::write(&filename, summary)?;
        log::info!("[SUMMARY] 📊 Trading summary saved to: {}", filename);
        Ok(())
    }
}
