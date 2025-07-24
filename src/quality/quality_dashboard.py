"""
Data Quality Dashboard - Day 5
Real-time monitoring dashboard for data quality and drift detection
"""

import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from loguru import logger

from .data_validation import DataValidator, DataProfiler, ValidationCheckpoint, create_financial_validation_suite
from .drift_detection import DriftDetector


class QualityDashboard:
    """Real-time data quality monitoring dashboard"""
    
    def __init__(self):
        self.validator = DataValidator("dashboard_validator")
        self.drift_detector = DriftDetector("dashboard_drift_detector")
        self.profiler = DataProfiler()
        self.checkpoint = None
        self._setup_dashboard()
        
    def _setup_dashboard(self):
        """Setup Streamlit dashboard configuration"""
        st.set_page_config(
            page_title="Data Quality Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
    def run_dashboard(self):
        """Main dashboard application"""
        st.title("🏦 Financial Data Quality Dashboard")
        st.markdown("Real-time monitoring of data quality, drift detection, and system health")
        
        # Sidebar controls
        self._render_sidebar()
        
        # Main content area
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔍 Validation", "📈 Drift Detection", "⚙️ System Health"])
        
        with tab1:
            self._render_overview_tab()
            
        with tab2:
            self._render_validation_tab()
            
        with tab3:
            self._render_drift_tab()
            
        with tab4:
            self._render_system_tab()
    
    def _render_sidebar(self):
        """Render sidebar controls"""
        st.sidebar.title("🎛️ Controls")
        
        # Data upload
        st.sidebar.subheader("📁 Data Upload")
        uploaded_file = st.sidebar.file_uploader(
            "Upload CSV file", 
            type=['csv'],
            help="Upload a CSV file for quality analysis"
        )
        
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            st.session_state['dashboard_data'] = df
            st.sidebar.success(f"✅ Loaded {len(df)} records")
        
        # Use sample data option
        if st.sidebar.button("📊 Use Sample Financial Data"):
            df = self._generate_sample_financial_data()
            st.session_state['dashboard_data'] = df
            st.sidebar.success(f"✅ Generated {len(df)} sample records")
        
        # Quality thresholds
        st.sidebar.subheader("⚙️ Quality Thresholds")
        completeness_threshold = st.sidebar.slider("Completeness", 0.0, 1.0, 0.95, 0.01)
        drift_significance = st.sidebar.slider("Drift Significance", 0.01, 0.10, 0.05, 0.01)
        
        st.session_state['completeness_threshold'] = completeness_threshold
        st.session_state['drift_significance'] = drift_significance
        
        # Monitoring controls
        st.sidebar.subheader("🔄 Monitoring")
        auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=False)
        
        if auto_refresh:
            time.sleep(30)
            st.rerun()
    
    def _render_overview_tab(self):
        """Render overview dashboard"""
        st.header("📊 Data Quality Overview")
        
        if 'dashboard_data' not in st.session_state:
            st.warning("⚠️ Please upload data or use sample data from the sidebar")
            return
        
        df = st.session_state['dashboard_data']
        
        # Quick stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Records", f"{len(df):,}")
        
        with col2:
            st.metric("📋 Total Columns", len(df.columns))
        
        with col3:
            missing_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
            st.metric("❓ Missing Data", f"{missing_ratio:.1%}")
        
        with col4:
            numeric_cols = len(df.select_dtypes(include=[np.number]).columns)
            st.metric("🔢 Numeric Columns", numeric_cols)
        
        # Data profile visualization
        st.subheader("📈 Data Profile")
        
        # Missing values heatmap
        if df.isnull().sum().sum() > 0:
            fig_missing = px.imshow(
                df.isnull().values,
                title="Missing Values Heatmap",
                labels=dict(x="Columns", y="Records", color="Missing"),
                aspect="auto"
            )
            st.plotly_chart(fig_missing, use_container_width=True)
        
        # Column statistics
        col1, col2 = st.columns(2)
        
        with col1:
            # Numeric column distributions
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            if len(numeric_columns) > 0:
                selected_numeric = st.selectbox("Select Numeric Column", numeric_columns)
                if selected_numeric:
                    fig_hist = px.histogram(
                        df, 
                        x=selected_numeric, 
                        title=f"Distribution of {selected_numeric}",
                        marginal="box"
                    )
                    st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Categorical column value counts
            categorical_columns = df.select_dtypes(include=['object']).columns
            if len(categorical_columns) > 0:
                selected_categorical = st.selectbox("Select Categorical Column", categorical_columns)
                if selected_categorical:
                    value_counts = df[selected_categorical].value_counts().head(10)
                    fig_bar = px.bar(
                        x=value_counts.index, 
                        y=value_counts.values,
                        title=f"Top Values in {selected_categorical}"
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
    
    def _render_validation_tab(self):
        """Render validation results"""
        st.header("🔍 Data Validation Results")
        
        if 'dashboard_data' not in st.session_state:
            st.warning("⚠️ Please upload data or use sample data from the sidebar")
            return
        
        df = st.session_state['dashboard_data']
        
        # Run validation
        if st.button("🚀 Run Validation Suite"):
            with st.spinner("Running validation checks..."):
                # Create financial validation suite
                suite = create_financial_validation_suite()
                
                # Setup checkpoint
                self.checkpoint = ValidationCheckpoint("dashboard_checkpoint", self.validator, suite)
                
                # Run validation
                results = self.checkpoint.run_checkpoint(df)
                st.session_state['validation_results'] = results
        
        # Display results
        if 'validation_results' in st.session_state:
            results = st.session_state['validation_results']
            
            # Summary metrics
            passed_checks = sum(1 for r in results.values() if r.passed)
            total_checks = len(results)
            quality_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("✅ Passed Checks", f"{passed_checks}/{total_checks}")
            
            with col2:
                st.metric("📊 Quality Score", f"{quality_score:.1f}%")
            
            with col3:
                avg_execution_time = np.mean([r.execution_time_ms for r in results.values()])
                st.metric("⏱️ Avg Execution Time", f"{avg_execution_time:.1f}ms")
            
            # Validation results table
            st.subheader("📋 Validation Details")
            
            results_data = []
            for check_name, result in results.items():
                results_data.append({
                    'Check': check_name,
                    'Status': '✅ Passed' if result.passed else '❌ Failed',
                    'Execution Time (ms)': f"{result.execution_time_ms:.1f}",
                    'Details': str(result.details)[:100] + "..." if len(str(result.details)) > 100 else str(result.details)
                })
            
            results_df = pd.DataFrame(results_data)
            st.dataframe(results_df, use_container_width=True)
            
            # Quality trend
            if self.checkpoint and len(self.checkpoint.results_history) > 1:
                st.subheader("📈 Quality Trend")
                
                trend_data = []
                for i, hist_results in enumerate(self.checkpoint.results_history):
                    passed = sum(1 for r in hist_results.values() if r.passed)
                    total = len(hist_results)
                    score = (passed / total) * 100 if total > 0 else 0
                    trend_data.append({'Run': i+1, 'Quality Score': score})
                
                trend_df = pd.DataFrame(trend_data)
                fig_trend = px.line(
                    trend_df, 
                    x='Run', 
                    y='Quality Score',
                    title="Quality Score Trend",
                    line_shape='spline'
                )
                st.plotly_chart(fig_trend, use_container_width=True)
    
    def _render_drift_tab(self):
        """Render drift detection results"""
        st.header("📈 Data Drift Detection")
        
        if 'dashboard_data' not in st.session_state:
            st.warning("⚠️ Please upload data or use sample data from the sidebar")
            return
        
        df = st.session_state['dashboard_data']
        
        # Feature selection for drift detection
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if len(numeric_columns) == 0:
            st.warning("⚠️ No numeric columns found for drift detection")
            return
        
        selected_features = st.multiselect(
            "Select Features for Drift Detection",
            numeric_columns,
            default=numeric_columns[:3] if len(numeric_columns) >= 3 else numeric_columns
        )
        
        # Drift detection method
        drift_method = st.selectbox(
            "Drift Detection Method",
            ["auto", "ks_test", "js_divergence", "population_stability", "adversarial"],
            help="Choose the statistical test for drift detection"
        )
        
        # Set reference and detect drift
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Set Reference Distribution"):
                with st.spinner("Setting reference distributions..."):
                    for feature in selected_features:
                        self.drift_detector.set_reference_distribution(df, feature)
                    st.success(f"✅ Set reference for {len(selected_features)} features")
        
        with col2:
            if st.button("🔍 Detect Drift"):
                if not self.drift_detector.reference_distributions:
                    st.error("❌ Please set reference distributions first")
                else:
                    with st.spinner("Detecting drift..."):
                        drift_results = {}
                        for feature in selected_features:
                            if feature in self.drift_detector.reference_distributions:
                                result = self.drift_detector.detect_drift(df, feature, drift_method)
                                drift_results[feature] = result
                        
                        st.session_state['drift_results'] = drift_results
        
        # Display drift results
        if 'drift_results' in st.session_state:
            drift_results = st.session_state['drift_results']
            
            # Summary metrics
            total_features = len(drift_results)
            drifted_features = sum(1 for r in drift_results.values() if r.drift_detected)
            avg_drift_score = np.mean([r.drift_score for r in drift_results.values()])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("🎯 Features Tested", total_features)
            
            with col2:
                st.metric("⚠️ Drift Detected", drifted_features)
            
            with col3:
                st.metric("📊 Avg Drift Score", f"{avg_drift_score:.3f}")
            
            # Drift results visualization
            st.subheader("📊 Drift Detection Results")
            
            # Create drift score chart
            features = list(drift_results.keys())
            scores = [drift_results[f].drift_score for f in features]
            detected = [drift_results[f].drift_detected for f in features]
            
            colors = ['red' if d else 'green' for d in detected]
            
            fig_drift = go.Figure(data=[
                go.Bar(x=features, y=scores, marker_color=colors)
            ])
            fig_drift.update_layout(
                title="Drift Scores by Feature",
                xaxis_title="Features",
                yaxis_title="Drift Score"
            )
            st.plotly_chart(fig_drift, use_container_width=True)
            
            # Detailed results table
            st.subheader("📋 Detailed Drift Results")
            
            drift_data = []
            for feature, result in drift_results.items():
                drift_data.append({
                    'Feature': feature,
                    'Drift Detected': '🚨 Yes' if result.drift_detected else '✅ No',
                    'Method': result.test_method,
                    'Drift Score': f"{result.drift_score:.4f}",
                    'P-Value': f"{result.p_value:.4f}" if result.p_value else "N/A",
                    'Threshold': f"{result.threshold:.4f}"
                })
            
            drift_df = pd.DataFrame(drift_data)
            st.dataframe(drift_df, use_container_width=True)
            
            # Drift history trend
            if len(self.drift_detector.drift_history) > 1:
                st.subheader("📈 Drift History")
                
                history_data = []
                for result in self.drift_detector.drift_history[-20:]:  # Last 20 results
                    history_data.append({
                        'Timestamp': result.timestamp,
                        'Feature': result.feature_name,
                        'Drift Score': result.drift_score,
                        'Drift Detected': result.drift_detected
                    })
                
                history_df = pd.DataFrame(history_data)
                
                if len(history_df) > 0:
                    fig_history = px.line(
                        history_df,
                        x='Timestamp',
                        y='Drift Score',
                        color='Feature',
                        title="Drift Score Timeline"
                    )
                    st.plotly_chart(fig_history, use_container_width=True)
    
    def _render_system_tab(self):
        """Render system health and performance metrics"""
        st.header("⚙️ System Health & Performance")
        
        # System metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            validation_count = len(self.validator.validation_results)
            st.metric("🔍 Total Validations", validation_count)
        
        with col2:
            drift_count = len(self.drift_detector.drift_history)
            st.metric("📈 Drift Tests", drift_count)
        
        with col3:
            profile_count = len(self.profiler.profiles)
            st.metric("📊 Data Profiles", profile_count)
        
        with col4:
            # Calculate uptime (dummy for demo)
            uptime_hours = 24  # Placeholder
            st.metric("⏰ Uptime", f"{uptime_hours}h")
        
        # Performance metrics
        st.subheader("⚡ Performance Metrics")
        
        if len(self.validator.validation_results) > 0:
            execution_times = [r.execution_time_ms for r in self.validator.validation_results]
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Execution time distribution
                fig_perf = px.histogram(
                    x=execution_times,
                    title="Validation Execution Time Distribution",
                    labels={'x': 'Execution Time (ms)', 'y': 'Count'}
                )
                st.plotly_chart(fig_perf, use_container_width=True)
            
            with col2:
                # Performance statistics
                perf_stats = {
                    'Metric': ['Mean', 'Median', 'P95', 'P99', 'Max'],
                    'Value (ms)': [
                        f"{np.mean(execution_times):.2f}",
                        f"{np.median(execution_times):.2f}",
                        f"{np.percentile(execution_times, 95):.2f}",
                        f"{np.percentile(execution_times, 99):.2f}",
                        f"{np.max(execution_times):.2f}"
                    ]
                }
                perf_df = pd.DataFrame(perf_stats)
                st.table(perf_df)
        
        # System configuration
        st.subheader("🛠️ Configuration")
        
        config_data = {
            'Setting': [
                'Validation Significance Level',
                'Drift Detection Method',
                'Profile Retention Days',
                'Auto Refresh Interval'
            ],
            'Value': [
                str(st.session_state.get('drift_significance', 0.05)),
                'Auto-select',
                '30 days',
                '30 seconds'
            ]
        }
        config_df = pd.DataFrame(config_data)
        st.table(config_df)
        
        # Export functionality
        st.subheader("📤 Export Data")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📊 Export Validation Results"):
                if 'validation_results' in st.session_state:
                    results_export = []
                    for name, result in st.session_state['validation_results'].items():
                        results_export.append(result.to_dict())
                    
                    st.download_button(
                        label="💾 Download JSON",
                        data=json.dumps(results_export, indent=2),
                        file_name=f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
        
        with col2:
            if st.button("📈 Export Drift Results"):
                if 'drift_results' in st.session_state:
                    drift_export = []
                    for name, result in st.session_state['drift_results'].items():
                        drift_export.append(result.to_dict())
                    
                    st.download_button(
                        label="💾 Download JSON",
                        data=json.dumps(drift_export, indent=2),
                        file_name=f"drift_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
        
        with col3:
            if st.button("📋 Export System Health"):
                health_data = {
                    'timestamp': datetime.now().isoformat(),
                    'validation_count': len(self.validator.validation_results),
                    'drift_count': len(self.drift_detector.drift_history),
                    'profile_count': len(self.profiler.profiles),
                    'configuration': config_data
                }
                
                st.download_button(
                    label="💾 Download JSON",
                    data=json.dumps(health_data, indent=2),
                    file_name=f"system_health_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
    
    def _generate_sample_financial_data(self) -> pd.DataFrame:
        """Generate sample financial data for demonstration"""
        np.random.seed(42)
        
        # Generate 1000 sample records
        dates = pd.date_range(start='2024-01-01', periods=1000, freq='D')
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'] * 200
        
        data = []
        for i, (date, symbol) in enumerate(zip(dates, symbols)):
            # Generate realistic financial data with some anomalies
            base_price = 100 + np.random.normal(0, 10)
            
            # Add trend
            base_price += i * 0.01
            
            # Add some anomalies (5% of data)
            if np.random.random() < 0.05:
                base_price *= np.random.uniform(0.7, 1.3)  # Price shock
            
            # OHLCV data
            open_price = base_price + np.random.normal(0, 1)
            high_price = max(open_price, base_price) + abs(np.random.normal(0, 2))
            low_price = min(open_price, base_price) - abs(np.random.normal(0, 2))
            close_price = base_price + np.random.normal(0, 1)
            volume = max(100000, int(np.random.lognormal(12, 0.5)))
            
            # Add some missing values (2% of data)
            if np.random.random() < 0.02:
                close_price = np.nan
            
            data.append({
                'date': date,
                'symbol': symbol,
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
                'market_cap': volume * close_price if not pd.isna(close_price) else np.nan
            })
        
        return pd.DataFrame(data)


def main():
    """Main function to run the dashboard"""
    dashboard = QualityDashboard()
    dashboard.run_dashboard()


if __name__ == "__main__":
    main()
