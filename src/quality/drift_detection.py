"""
Data drift detection module for identifying changes in data distributions.
Implements multiple statistical methods for comprehensive drift analysis.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import jensenshannon
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from loguru import logger
from prometheus_client import Counter, Histogram, Gauge


@dataclass
class DriftResult:
    """Result of drift detection analysis"""
    feature_name: str
    drift_detected: bool
    drift_score: float
    p_value: Optional[float]
    test_statistic: Optional[float]
    test_method: str
    threshold: float
    timestamp: datetime
    reference_stats: Dict[str, float]
    current_stats: Dict[str, float]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'feature_name': self.feature_name,
            'drift_detected': self.drift_detected,
            'drift_score': self.drift_score,
            'p_value': self.p_value,
            'test_statistic': self.test_statistic,
            'test_method': self.test_method,
            'threshold': self.threshold,
            'timestamp': self.timestamp.isoformat(),
            'reference_stats': self.reference_stats,
            'current_stats': self.current_stats
        }


@dataclass
class DistributionSnapshot:
    """Snapshot of data distribution for comparison"""
    feature_name: str
    timestamp: datetime
    sample_size: int
    statistics: Dict[str, float]
    histogram_bins: List[float]
    histogram_counts: List[int]
    data_hash: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'feature_name': self.feature_name,
            'timestamp': self.timestamp.isoformat(),
            'sample_size': self.sample_size,
            'statistics': self.statistics,
            'histogram_bins': self.histogram_bins,
            'histogram_counts': self.histogram_counts,
            'data_hash': self.data_hash
        }


class DriftDetector:
    """Comprehensive data drift detection system"""
    
    def __init__(self, name: str = "default", significance_level: float = 0.05):
        self.name = name
        self.significance_level = significance_level
        self.reference_distributions: Dict[str, DistributionSnapshot] = {}
        self.drift_history: List[DriftResult] = []
        self._setup_metrics()
        
    def _setup_metrics(self):
        """Setup Prometheus metrics for drift tracking"""
        self.drift_tests_run = Counter(
            'drift_tests_total', 
            'Total drift tests run', 
            ['detector', 'feature', 'method']
        )
        self.drift_detections = Counter(
            'drift_detected_total', 
            'Total drift detections', 
            ['detector', 'feature', 'method']
        )
        self.drift_score = Histogram(
            'drift_score_distribution', 
            'Distribution of drift scores'
        )
        self.drift_severity = Gauge(
            'drift_severity_score', 
            'Current drift severity score', 
            ['detector', 'feature']
        )
        
    def set_reference_distribution(self, df: pd.DataFrame, feature_name: str):
        """Set reference distribution for drift detection"""
        if feature_name not in df.columns:
            raise ValueError(f"Feature {feature_name} not found in dataset")
        
        data = df[feature_name].dropna()
        
        # Calculate statistical snapshot
        statistics = self._calculate_statistics(data)
        
        # Create histogram for distribution comparison
        if pd.api.types.is_numeric_dtype(data):
            hist_counts, hist_bins = np.histogram(data, bins=50)
            hist_bins = hist_bins.tolist()
            hist_counts = hist_counts.tolist()
        else:
            # For categorical data, use value counts
            value_counts = data.value_counts()
            hist_bins = value_counts.index.tolist()
            hist_counts = value_counts.values.tolist()
        
        # Create data hash for integrity checking
        data_hash = self._calculate_data_hash(data)
        
        snapshot = DistributionSnapshot(
            feature_name=feature_name,
            timestamp=datetime.now(),
            sample_size=len(data),
            statistics=statistics,
            histogram_bins=hist_bins,
            histogram_counts=hist_counts,
            data_hash=data_hash
        )
        
        self.reference_distributions[feature_name] = snapshot
        logger.info(f"Set reference distribution for {feature_name} ({len(data)} samples)")
        
    def detect_drift(self, df: pd.DataFrame, feature_name: str, 
                    method: str = "auto") -> DriftResult:
        """Detect drift in specified feature"""
        if feature_name not in self.reference_distributions:
            raise ValueError(f"No reference distribution set for {feature_name}")
        
        if feature_name not in df.columns:
            raise ValueError(f"Feature {feature_name} not found in current dataset")
        
        reference = self.reference_distributions[feature_name]
        current_data = df[feature_name].dropna()
        
        # Auto-select method based on data type
        if method == "auto":
            if pd.api.types.is_numeric_dtype(current_data):
                method = "ks_test"
            else:
                method = "chi_square"
        
        # Run drift detection
        if method == "ks_test":
            result = self._ks_test_drift(reference, current_data)
        elif method == "chi_square":
            result = self._chi_square_drift(reference, current_data)
        elif method == "js_divergence":
            result = self._js_divergence_drift(reference, current_data)
        elif method == "population_stability":
            result = self._population_stability_drift(reference, current_data)
        elif method == "adversarial":
            result = self._adversarial_drift(reference, current_data)
        else:
            raise ValueError(f"Unknown drift detection method: {method}")
        
        # Update metrics
        self.drift_tests_run.labels(
            detector=self.name, 
            feature=feature_name, 
            method=method
        ).inc()
        
        if result.drift_detected:
            self.drift_detections.labels(
                detector=self.name, 
                feature=feature_name, 
                method=method
            ).inc()
        
        self.drift_score.observe(result.drift_score)
        self.drift_severity.labels(
            detector=self.name, 
            feature=feature_name
        ).set(result.drift_score)
        
        # Store result
        self.drift_history.append(result)
        
        return result
    
    def _ks_test_drift(self, reference: DistributionSnapshot, 
                      current_data: pd.Series) -> DriftResult:
        """Kolmogorov-Smirnov test for numerical data drift"""
        # Reconstruct reference data from histogram (approximation)
        ref_bins = np.array(reference.histogram_bins)
        ref_counts = np.array(reference.histogram_counts)
        
        # Create reference sample
        ref_sample = []
        for i in range(len(ref_counts)):
            if i < len(ref_bins) - 1:
                bin_center = (ref_bins[i] + ref_bins[i+1]) / 2
                ref_sample.extend([bin_center] * ref_counts[i])
        
        ref_sample = np.array(ref_sample)
        current_sample = current_data.values
        
        # Perform KS test
        try:
            statistic, p_value = stats.ks_2samp(ref_sample, current_sample)
            drift_detected = p_value < self.significance_level
            drift_score = float(statistic)
        except (ValueError, TypeError) as e:
            logger.warning(f"KS test failed: {e}")
            statistic, p_value = 0.0, 1.0
            drift_detected = False
            drift_score = 0.0
        
        return DriftResult(
            feature_name=reference.feature_name,
            drift_detected=drift_detected,
            drift_score=drift_score,
            p_value=float(p_value),
            test_statistic=float(statistic),
            test_method="ks_test",
            threshold=self.significance_level,
            timestamp=datetime.now(),
            reference_stats=reference.statistics,
            current_stats=self._calculate_statistics(current_data)
        )
    
    def _chi_square_drift(self, reference: DistributionSnapshot, 
                         current_data: pd.Series) -> DriftResult:
        """Chi-square test for categorical data drift"""
        # Get reference and current frequency distributions
        ref_bins = reference.histogram_bins
        ref_counts = np.array(reference.histogram_counts)
        
        # Get current value counts
        current_counts = current_data.value_counts()
        
        # Align categories
        all_categories = set(ref_bins) | set(current_counts.index)
        
        ref_frequencies = []
        current_frequencies = []
        
        for category in all_categories:
            ref_freq = ref_counts[ref_bins.index(category)] if category in ref_bins else 0
            curr_freq = current_counts.get(category, 0)
            
            ref_frequencies.append(ref_freq)
            current_frequencies.append(curr_freq)
        
        ref_frequencies = np.array(ref_frequencies)
        current_frequencies = np.array(current_frequencies)
        
        # Normalize to same total
        ref_total = ref_frequencies.sum()
        curr_total = current_frequencies.sum()
        
        if ref_total > 0 and curr_total > 0:
            expected_frequencies = ref_frequencies * (curr_total / ref_total)
            
            # Avoid division by zero
            expected_frequencies = np.maximum(expected_frequencies, 1e-10)
            
            try:
                statistic, p_value = stats.chisquare(current_frequencies, expected_frequencies)
                drift_detected = p_value < self.significance_level
                drift_score = float(statistic / len(all_categories))  # Normalized
            except (ValueError, TypeError) as e:
                logger.warning(f"Chi-square test failed: {e}")
                statistic, p_value = 0.0, 1.0
                drift_detected = False
                drift_score = 0.0
        else:
            statistic, p_value = 0.0, 1.0
            drift_detected = False
            drift_score = 0.0
        
        return DriftResult(
            feature_name=reference.feature_name,
            drift_detected=drift_detected,
            drift_score=drift_score,
            p_value=float(p_value),
            test_statistic=float(statistic),
            test_method="chi_square",
            threshold=self.significance_level,
            timestamp=datetime.now(),
            reference_stats=reference.statistics,
            current_stats=self._calculate_statistics(current_data)
        )
    
    def _js_divergence_drift(self, reference: DistributionSnapshot, 
                           current_data: pd.Series) -> DriftResult:
        """Jensen-Shannon divergence for distribution comparison"""
        # Create probability distributions
        ref_counts = np.array(reference.histogram_counts, dtype=float)
        ref_probs = ref_counts / ref_counts.sum() if ref_counts.sum() > 0 else ref_counts
        
        # Create current histogram with same bins
        if pd.api.types.is_numeric_dtype(current_data):
            ref_bins = np.array(reference.histogram_bins)
            current_counts, _ = np.histogram(current_data, bins=ref_bins)
        else:
            # For categorical data
            ref_categories = reference.histogram_bins
            current_value_counts = current_data.value_counts()
            current_counts = np.array([
                current_value_counts.get(cat, 0) for cat in ref_categories
            ])
        
        current_probs = current_counts / current_counts.sum() if current_counts.sum() > 0 else current_counts
        
        # Ensure same length
        min_len = min(len(ref_probs), len(current_probs))
        ref_probs = ref_probs[:min_len]
        current_probs = current_probs[:min_len]
        
        # Calculate JS divergence
        try:
            js_distance = jensenshannon(ref_probs, current_probs)
            drift_score = float(js_distance)
            drift_detected = drift_score > 0.1  # Threshold for JS divergence
        except (ValueError, TypeError) as e:
            logger.warning(f"JS divergence calculation failed: {e}")
            drift_score = 0.0
            drift_detected = False
        
        return DriftResult(
            feature_name=reference.feature_name,
            drift_detected=drift_detected,
            drift_score=drift_score,
            p_value=None,
            test_statistic=drift_score,
            test_method="js_divergence",
            threshold=0.1,
            timestamp=datetime.now(),
            reference_stats=reference.statistics,
            current_stats=self._calculate_statistics(current_data)
        )
    
    def _population_stability_drift(self, reference: DistributionSnapshot, 
                                  current_data: pd.Series) -> DriftResult:
        """Population Stability Index (PSI) for drift detection"""
        # Create bins and calculate PSI
        if pd.api.types.is_numeric_dtype(current_data):
            # Use reference bins
            ref_bins = np.array(reference.histogram_bins)
            ref_counts = np.array(reference.histogram_counts, dtype=float)
            
            # Calculate current distribution
            current_counts, _ = np.histogram(current_data, bins=ref_bins)
            current_counts = current_counts.astype(float)
            
            # Normalize to probabilities
            ref_probs = ref_counts / ref_counts.sum() if ref_counts.sum() > 0 else ref_counts
            current_probs = current_counts / current_counts.sum() if current_counts.sum() > 0 else current_counts
            
            # Avoid log(0) by adding small epsilon
            epsilon = 1e-10
            ref_probs = np.maximum(ref_probs, epsilon)
            current_probs = np.maximum(current_probs, epsilon)
            
            # Calculate PSI
            psi = np.sum((current_probs - ref_probs) * np.log(current_probs / ref_probs))
            
        else:
            # For categorical data
            ref_categories = reference.histogram_bins
            ref_counts = np.array(reference.histogram_counts, dtype=float)
            
            current_value_counts = current_data.value_counts()
            current_counts = np.array([
                current_value_counts.get(cat, 0) for cat in ref_categories
            ], dtype=float)
            
            # Normalize
            ref_probs = ref_counts / ref_counts.sum() if ref_counts.sum() > 0 else ref_counts
            current_probs = current_counts / current_counts.sum() if current_counts.sum() > 0 else current_counts
            
            # Calculate PSI
            epsilon = 1e-10
            ref_probs = np.maximum(ref_probs, epsilon)
            current_probs = np.maximum(current_probs, epsilon)
            
            psi = np.sum((current_probs - ref_probs) * np.log(current_probs / ref_probs))
        
        drift_score = float(abs(psi))
        drift_detected = drift_score > 0.2  # Standard PSI threshold
        
        return DriftResult(
            feature_name=reference.feature_name,
            drift_detected=drift_detected,
            drift_score=drift_score,
            p_value=None,
            test_statistic=drift_score,
            test_method="population_stability",
            threshold=0.2,
            timestamp=datetime.now(),
            reference_stats=reference.statistics,
            current_stats=self._calculate_statistics(current_data)
        )
    
    def _adversarial_drift(self, reference: DistributionSnapshot, 
                          current_data: pd.Series) -> DriftResult:
        """Adversarial drift detection using classifier accuracy"""
        try:
            # Prepare data for binary classification
            ref_sample_size = min(reference.sample_size, 1000)  # Limit for performance
            current_sample_size = min(len(current_data), 1000)
            
            # Sample data
            if pd.api.types.is_numeric_dtype(current_data):
                # For numeric data, use values directly
                ref_values = np.random.choice(
                    reference.histogram_bins[:-1], 
                    size=ref_sample_size, 
                    p=np.array(reference.histogram_counts)/sum(reference.histogram_counts)
                ).reshape(-1, 1)
                current_values = current_data.sample(n=current_sample_size).values.reshape(-1, 1)
            else:
                # For categorical data, encode categories
                all_categories = list(set(reference.histogram_bins) | set(current_data.unique()))
                category_map = {cat: i for i, cat in enumerate(all_categories)}
                
                # Sample reference data
                ref_categories = np.random.choice(
                    reference.histogram_bins, 
                    size=ref_sample_size,
                    p=np.array(reference.histogram_counts)/sum(reference.histogram_counts)
                )
                ref_values = np.array([category_map[cat] for cat in ref_categories]).reshape(-1, 1)
                
                # Sample current data
                current_sample = current_data.sample(n=current_sample_size)
                current_values = np.array([category_map[cat] for cat in current_sample]).reshape(-1, 1)
            
            # Create labels (0 for reference, 1 for current)
            X = np.vstack([ref_values, current_values])
            y = np.hstack([np.zeros(len(ref_values)), np.ones(len(current_values))])
            
            # Train classifier
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
            
            clf = RandomForestClassifier(n_estimators=50, random_state=42)
            clf.fit(X_train, y_train)
            
            # Test classifier
            y_pred = clf.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            # If classifier can distinguish well (high accuracy), there's drift
            drift_score = float(abs(accuracy - 0.5) * 2)  # Scale to 0-1
            drift_detected = accuracy > 0.75  # If >75% accuracy, significant drift
            
        except (ValueError, TypeError, ImportError) as e:
            logger.warning(f"Adversarial drift detection failed: {e}")
            drift_score = 0.0
            drift_detected = False
            accuracy = 0.5
        
        return DriftResult(
            feature_name=reference.feature_name,
            drift_detected=drift_detected,
            drift_score=drift_score,
            p_value=None,
            test_statistic=float(accuracy),
            test_method="adversarial",
            threshold=0.75,
            timestamp=datetime.now(),
            reference_stats=reference.statistics,
            current_stats=self._calculate_statistics(current_data)
        )
    
    def _calculate_statistics(self, data: pd.Series) -> Dict[str, float]:
        """Calculate comprehensive statistics for a data series"""
        stats_dict = {}
        
        if pd.api.types.is_numeric_dtype(data):
            # Numeric statistics
            stats_dict.update({
                'mean': float(data.mean()),
                'median': float(data.median()),
                'std': float(data.std()),
                'min': float(data.min()),
                'max': float(data.max()),
                'q25': float(data.quantile(0.25)),
                'q75': float(data.quantile(0.75)),
                'skewness': float(data.skew()),
                'kurtosis': float(data.kurtosis())
            })
        else:
            # Categorical statistics
            value_counts = data.value_counts()
            stats_dict.update({
                'unique_count': int(data.nunique()),
                'most_frequent': str(value_counts.index[0]) if len(value_counts) > 0 else None,
                'most_frequent_ratio': float(value_counts.iloc[0] / len(data)) if len(value_counts) > 0 else 0.0,
                'entropy': float(-np.sum((value_counts / len(data)) * np.log2(value_counts / len(data))))
            })
        
        return stats_dict
    
    def _calculate_data_hash(self, data: pd.Series) -> str:
        """Calculate hash of data for integrity checking"""
        import hashlib
        data_str = str(sorted(data.dropna().values.tolist()))
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def detect_multivariate_drift(self, df: pd.DataFrame, features: List[str], 
                                 method: str = "energy") -> DriftResult:
        """Detect multivariate drift across multiple features"""
        if not all(f in df.columns for f in features):
            missing = [f for f in features if f not in df.columns]
            raise ValueError(f"Features not found: {missing}")
        
        # For simplicity, aggregate individual drift scores
        individual_results = []
        for feature in features:
            if feature in self.reference_distributions:
                result = self.detect_drift(df, feature, method="auto")
                individual_results.append(result)
        
        if not individual_results:
            raise ValueError("No reference distributions set for any features")
        
        # Aggregate drift scores
        avg_drift_score = np.mean([r.drift_score for r in individual_results])
        max_drift_score = np.max([r.drift_score for r in individual_results])
        drift_detected = any(r.drift_detected for r in individual_results)
        
        # Use maximum drift score as overall score
        overall_result = DriftResult(
            feature_name="multivariate_" + "_".join(features),
            drift_detected=drift_detected,
            drift_score=float(max_drift_score),
            p_value=None,
            test_statistic=float(avg_drift_score),
            test_method=f"multivariate_{method}",
            threshold=0.1,
            timestamp=datetime.now(),
            reference_stats={'features': features, 'avg_score': float(avg_drift_score)},
            current_stats={'max_score': float(max_drift_score), 'detected_count': sum(r.drift_detected for r in individual_results)}
        )
        
        return overall_result
    
    def get_drift_summary(self, days: int = 7) -> Dict[str, Any]:
        """Get summary of drift detection over time period"""
        cutoff_time = datetime.now() - timedelta(days=days)
        recent_results = [r for r in self.drift_history if r.timestamp > cutoff_time]
        
        if not recent_results:
            return {'message': 'No drift results in specified time period'}
        
        # Group by feature
        features = {}
        for result in recent_results:
            if result.feature_name not in features:
                features[result.feature_name] = []
            features[result.feature_name].append(result)
        
        summary = {
            'time_period_days': days,
            'total_tests': len(recent_results),
            'drift_detections': sum(1 for r in recent_results if r.drift_detected),
            'features_tested': len(features),
            'feature_summary': {}
        }
        
        for feature_name, results in features.items():
            drift_count = sum(1 for r in results if r.drift_detected)
            avg_score = np.mean([r.drift_score for r in results])
            
            summary['feature_summary'][feature_name] = {
                'tests_run': len(results),
                'drift_detections': drift_count,
                'drift_rate': drift_count / len(results),
                'avg_drift_score': float(avg_score),
                'last_test': results[-1].timestamp.isoformat()
            }
        
        return summary
