"""
Data Quality Incident Response System
Automated alerting and response for data quality issues
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from dataclasses import dataclass
from enum import Enum


class IncidentSeverity(Enum):
    """Incident severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ResponseAction(Enum):
    """Available response actions"""
    NOTIFY_TEAM = "notify_team"
    PAUSE_PIPELINE = "pause_pipeline"
    FALLBACK_DATA = "fallback_data"
    ESCALATE = "escalate"
    AUTO_REMEDIATE = "auto_remediate"


@dataclass
class QualityIncident:
    """Data quality incident representation"""
    incident_id: str
    timestamp: datetime
    severity: IncidentSeverity
    title: str
    description: str
    affected_datasets: List[str]
    quality_score: float
    failed_checks: List[str]
    drift_detections: int
    metadata: Dict[str, Any]


@dataclass
class ResponseRule:
    """Automated response rule configuration"""
    name: str
    conditions: Dict[str, Any]
    actions: List[ResponseAction]
    severity_threshold: IncidentSeverity
    cooldown_minutes: int = 30


class IncidentResponseSystem:
    """Automated incident response and alerting system"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.active_incidents: Dict[str, QualityIncident] = {}
        self.response_rules = self._setup_default_rules()
        self.action_history: List[Dict[str, Any]] = []
        
        # Create incident storage
        self.incidents_dir = Path("logs/incidents")
        self.incidents_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("Incident Response System initialized")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load incident response configuration"""
        default_config = {
            "quality_thresholds": {
                "critical": 60.0,
                "high": 75.0,
                "medium": 85.0
            },
            "drift_thresholds": {
                "critical": 5,
                "high": 3,
                "medium": 2
            },
            "notification": {
                "email_enabled": False,
                "webhook_enabled": False,
                "teams": {
                    "data_engineers": ["engineer1@company.com", "engineer2@company.com"],
                    "data_scientists": ["scientist1@company.com", "scientist2@company.com"],
                    "management": ["manager@company.com"]
                }
            },
            "response": {
                "auto_pause_pipeline": True,
                "auto_fallback": True,
                "escalation_timeout_minutes": 60
            }
        }
        
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r') as f:
                    custom_config = json.load(f)
                default_config.update(custom_config)
            except Exception as e:
                logger.warning(f"Failed to load config from {config_path}: {e}")
        
        return default_config
    
    def _setup_default_rules(self) -> List[ResponseRule]:
        """Setup default response rules"""
        return [
            ResponseRule(
                name="critical_quality_failure",
                conditions={"quality_score": {"lt": 60.0}},
                actions=[
                    ResponseAction.PAUSE_PIPELINE,
                    ResponseAction.FALLBACK_DATA,
                    ResponseAction.NOTIFY_TEAM,
                    ResponseAction.ESCALATE
                ],
                severity_threshold=IncidentSeverity.CRITICAL,
                cooldown_minutes=15
            ),
            ResponseRule(
                name="high_drift_detection",
                conditions={"drift_detections": {"gte": 5}},
                actions=[
                    ResponseAction.NOTIFY_TEAM,
                    ResponseAction.AUTO_REMEDIATE
                ],
                severity_threshold=IncidentSeverity.HIGH,
                cooldown_minutes=30
            ),
            ResponseRule(
                name="multiple_failed_checks",
                conditions={"failed_checks_count": {"gte": 3}},
                actions=[
                    ResponseAction.NOTIFY_TEAM,
                    ResponseAction.PAUSE_PIPELINE
                ],
                severity_threshold=IncidentSeverity.MEDIUM,
                cooldown_minutes=45
            )
        ]
    
    async def assess_quality_incident(
        self,
        validation_results: Dict[str, Any],
        drift_results: Dict[str, Any],
        dataset_name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[QualityIncident]:
        """Assess if current state constitutes an incident"""
        
        # Calculate quality metrics
        quality_score = self._calculate_quality_score(validation_results)
        failed_checks = self._get_failed_checks(validation_results)
        drift_count = self._count_drift_detections(drift_results)
        
        # Determine severity
        severity = self._determine_severity(quality_score, drift_count, len(failed_checks))
        
        # Check if incident threshold is met
        if severity == IncidentSeverity.LOW:
            return None  # No incident
        
        # Create incident
        incident_id = f"INC-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{dataset_name}"
        
        incident = QualityIncident(
            incident_id=incident_id,
            timestamp=datetime.now(),
            severity=severity,
            title=f"Data Quality Issue Detected in {dataset_name}",
            description=self._generate_incident_description(
                quality_score, failed_checks, drift_count
            ),
            affected_datasets=[dataset_name],
            quality_score=quality_score,
            failed_checks=failed_checks,
            drift_detections=drift_count,
            metadata=metadata or {}
        )
        
        logger.warning(f"🚨 Quality incident detected: {incident_id} ({severity.value})")
        return incident
    
    async def handle_incident(self, incident: QualityIncident) -> Dict[str, Any]:
        """Handle a quality incident with automated response"""
        logger.info(f"🔄 Handling incident: {incident.incident_id}")
        
        # Store incident
        self.active_incidents[incident.incident_id] = incident
        await self._save_incident(incident)
        
        # Find applicable response rules
        applicable_rules = self._find_applicable_rules(incident)
        
        # Execute response actions
        response_log = {
            "incident_id": incident.incident_id,
            "timestamp": datetime.now().isoformat(),
            "actions_taken": [],
            "notifications_sent": [],
            "errors": []
        }
        
        for rule in applicable_rules:
            logger.info(f"Applying rule: {rule.name}")
            
            for action in rule.actions:
                try:
                    action_result = await self._execute_action(action, incident)
                    response_log["actions_taken"].append({
                        "action": action.value,
                        "rule": rule.name,
                        "result": action_result,
                        "timestamp": datetime.now().isoformat()
                    })
                    logger.info(f"✅ Action {action.value} completed")
                    
                except Exception as e:
                    error_msg = f"Failed to execute {action.value}: {e}"
                    response_log["errors"].append(error_msg)
                    logger.error(error_msg)
        
        # Store action history
        self.action_history.append(response_log)
        
        # Save response log
        await self._save_response_log(response_log)
        
        logger.success(f"✅ Incident {incident.incident_id} handled successfully")
        return response_log
    
    async def _execute_action(
        self, 
        action: ResponseAction, 
        incident: QualityIncident
    ) -> Dict[str, Any]:
        """Execute a specific response action"""
        
        if action == ResponseAction.NOTIFY_TEAM:
            return await self._notify_team(incident)
        
        elif action == ResponseAction.PAUSE_PIPELINE:
            return await self._pause_pipeline(incident)
        
        elif action == ResponseAction.FALLBACK_DATA:
            return await self._activate_fallback_data(incident)
        
        elif action == ResponseAction.ESCALATE:
            return await self._escalate_incident(incident)
        
        elif action == ResponseAction.AUTO_REMEDIATE:
            return await self._auto_remediate(incident)
        
        else:
            raise ValueError(f"Unknown action: {action}")
    
    async def _notify_team(self, incident: QualityIncident) -> Dict[str, Any]:
        """Send notifications to relevant teams"""
        notifications = []
        
        # Determine notification recipients based on severity
        if incident.severity in [IncidentSeverity.CRITICAL, IncidentSeverity.HIGH]:
            teams = ["data_engineers", "management"]
        else:
            teams = ["data_engineers"]
        
        # Console notification (always available)
        console_msg = self._format_console_notification(incident)
        print(console_msg)
        notifications.append({"type": "console", "status": "sent"})
        
        # Email notifications (if enabled)
        if self.config["notification"]["email_enabled"]:
            try:
                for team in teams:
                    recipients = self.config["notification"]["teams"].get(team, [])
                    if recipients:
                        await self._send_email_notification(incident, recipients)
                        notifications.append({
                            "type": "email",
                            "team": team,
                            "recipients": len(recipients),
                            "status": "sent"
                        })
            except Exception as e:
                notifications.append({"type": "email", "status": "failed", "error": str(e)})
        
        # Webhook notifications (if enabled)
        if self.config["notification"]["webhook_enabled"]:
            try:
                await self._send_webhook_notification(incident)
                notifications.append({"type": "webhook", "status": "sent"})
            except Exception as e:
                notifications.append({"type": "webhook", "status": "failed", "error": str(e)})
        
        return {"notifications": notifications}
    
    async def _pause_pipeline(self, incident: QualityIncident) -> Dict[str, Any]:
        """Pause data pipeline"""
        if self.config["response"]["auto_pause_pipeline"]:
            # In a real implementation, this would call pipeline management APIs
            logger.warning("🛑 Data pipeline paused due to quality incident")
            
            # Create pause indicator file
            pause_file = Path("logs/pipeline_paused.flag")
            with open(pause_file, 'w') as f:
                json.dump({
                    "paused_at": datetime.now().isoformat(),
                    "incident_id": incident.incident_id,
                    "reason": "Data quality incident"
                }, f, indent=2)
            
            return {"status": "paused", "pause_file": str(pause_file)}
        else:
            return {"status": "skipped", "reason": "auto_pause_pipeline disabled"}
    
    async def _activate_fallback_data(self, incident: QualityIncident) -> Dict[str, Any]:
        """Activate fallback to last known good data"""
        if self.config["response"]["auto_fallback"]:
            # In a real implementation, this would activate fallback mechanisms
            logger.info("🔄 Activating fallback to last known good data")
            
            # Create fallback indicator
            fallback_file = Path("logs/fallback_active.flag")
            with open(fallback_file, 'w') as f:
                json.dump({
                    "activated_at": datetime.now().isoformat(),
                    "incident_id": incident.incident_id,
                    "fallback_reason": "Data quality degradation"
                }, f, indent=2)
            
            return {"status": "activated", "fallback_file": str(fallback_file)}
        else:
            return {"status": "skipped", "reason": "auto_fallback disabled"}
    
    async def _escalate_incident(self, incident: QualityIncident) -> Dict[str, Any]:
        """Escalate incident to management"""
        logger.warning(f"📢 Escalating incident {incident.incident_id} to management")
        
        # In a real implementation, this would integrate with incident management systems
        escalation_file = Path("logs/escalations") / f"{incident.incident_id}_escalation.json"
        escalation_file.parent.mkdir(parents=True, exist_ok=True)
        
        escalation_data = {
            "incident_id": incident.incident_id,
            "escalated_at": datetime.now().isoformat(),
            "severity": incident.severity.value,
            "summary": incident.description,
            "escalation_reason": "Automated escalation due to severity level"
        }
        
        with open(escalation_file, 'w') as f:
            json.dump(escalation_data, f, indent=2)
        
        return {"status": "escalated", "escalation_file": str(escalation_file)}
    
    async def _auto_remediate(self, incident: QualityIncident) -> Dict[str, Any]:
        """Attempt automatic remediation"""
        logger.info(f"🔧 Attempting auto-remediation for {incident.incident_id}")
        
        remediation_actions = []
        
        # Data cleaning remediation
        if "missing_values" in incident.failed_checks:
            remediation_actions.append("data_imputation")
        
        if "outliers" in incident.failed_checks:
            remediation_actions.append("outlier_removal")
        
        if incident.drift_detections > 0:
            remediation_actions.append("drift_correction")
        
        # In a real implementation, these would trigger actual remediation processes
        logger.info(f"Remediation actions planned: {', '.join(remediation_actions)}")
        
        return {
            "status": "initiated",
            "actions": remediation_actions,
            "estimated_completion": (datetime.now() + timedelta(minutes=15)).isoformat()
        }
    
    def _calculate_quality_score(self, validation_results: Dict[str, Any]) -> float:
        """Calculate overall quality score from validation results"""
        if not validation_results:
            return 0.0
        
        passed_count = sum(1 for result in validation_results.values() if result.passed)
        return (passed_count / len(validation_results)) * 100
    
    def _get_failed_checks(self, validation_results: Dict[str, Any]) -> List[str]:
        """Get list of failed validation checks"""
        return [name for name, result in validation_results.items() if not result.passed]
    
    def _count_drift_detections(self, drift_results: Dict[str, Any]) -> int:
        """Count total drift detections across all methods"""
        total_drift = 0
        for method_results in drift_results.values():
            for result in method_results.values():
                if result.drift_detected:
                    total_drift += 1
        return total_drift
    
    def _determine_severity(
        self, 
        quality_score: float, 
        drift_count: int, 
        failed_checks: int
    ) -> IncidentSeverity:
        """Determine incident severity based on metrics"""
        
        # Critical conditions
        if (quality_score < self.config["quality_thresholds"]["critical"] or
            drift_count >= self.config["drift_thresholds"]["critical"]):
            return IncidentSeverity.CRITICAL
        
        # High severity conditions
        if (quality_score < self.config["quality_thresholds"]["high"] or
            drift_count >= self.config["drift_thresholds"]["high"]):
            return IncidentSeverity.HIGH
        
        # Medium severity conditions
        if (quality_score < self.config["quality_thresholds"]["medium"] or
            drift_count >= self.config["drift_thresholds"]["medium"] or
            failed_checks >= 3):
            return IncidentSeverity.MEDIUM
        
        return IncidentSeverity.LOW
    
    def _generate_incident_description(
        self, 
        quality_score: float, 
        failed_checks: List[str], 
        drift_count: int
    ) -> str:
        """Generate human-readable incident description"""
        description_parts = []
        
        description_parts.append(f"Quality score: {quality_score:.1f}%")
        
        if failed_checks:
            description_parts.append(f"Failed checks: {', '.join(failed_checks[:3])}")
            if len(failed_checks) > 3:
                description_parts.append(f"and {len(failed_checks) - 3} more")
        
        if drift_count > 0:
            description_parts.append(f"Drift detections: {drift_count}")
        
        return " | ".join(description_parts)
    
    def _find_applicable_rules(self, incident: QualityIncident) -> List[ResponseRule]:
        """Find response rules applicable to the incident"""
        applicable_rules = []
        
        for rule in self.response_rules:
            if self._evaluate_rule_conditions(rule.conditions, incident):
                applicable_rules.append(rule)
        
        return applicable_rules
    
    def _evaluate_rule_conditions(
        self, 
        conditions: Dict[str, Any], 
        incident: QualityIncident
    ) -> bool:
        """Evaluate if rule conditions are met"""
        
        for condition_key, condition_value in conditions.items():
            if condition_key == "quality_score":
                if not self._compare_value(incident.quality_score, condition_value):
                    return False
            
            elif condition_key == "drift_detections":
                if not self._compare_value(incident.drift_detections, condition_value):
                    return False
            
            elif condition_key == "failed_checks_count":
                if not self._compare_value(len(incident.failed_checks), condition_value):
                    return False
        
        return True
    
    def _compare_value(self, actual_value: float, condition: Dict[str, Any]) -> bool:
        """Compare actual value against condition"""
        if "lt" in condition:
            return actual_value < condition["lt"]
        if "lte" in condition:
            return actual_value <= condition["lte"]
        if "gt" in condition:
            return actual_value > condition["gt"]
        if "gte" in condition:
            return actual_value >= condition["gte"]
        if "eq" in condition:
            return actual_value == condition["eq"]
        return True
    
    def _format_console_notification(self, incident: QualityIncident) -> str:
        """Format console notification message"""
        severity_emoji = {
            IncidentSeverity.CRITICAL: "🚨",
            IncidentSeverity.HIGH: "⚠️",
            IncidentSeverity.MEDIUM: "🔶",
            IncidentSeverity.LOW: "🔵"
        }
        
        return f"""
{severity_emoji[incident.severity]} DATA QUALITY INCIDENT ALERT {severity_emoji[incident.severity]}

Incident ID: {incident.incident_id}
Severity: {incident.severity.value}
Time: {incident.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

Title: {incident.title}
Description: {incident.description}

Affected Datasets: {', '.join(incident.affected_datasets)}
Quality Score: {incident.quality_score:.1f}%
Failed Checks: {len(incident.failed_checks)}
Drift Detections: {incident.drift_detections}

Automated response initiated...
        """.strip()
    
    async def _send_email_notification(
        self, 
        incident: QualityIncident, 
        recipients: List[str]
    ) -> None:
        """Send email notification (placeholder implementation)"""
        # In a real implementation, this would use actual SMTP configuration
        logger.info(f"📧 Email notification sent to {len(recipients)} recipients")
        # Implementation would go here
    
    async def _send_webhook_notification(self, incident: QualityIncident) -> None:
        """Send webhook notification (placeholder implementation)"""
        # In a real implementation, this would call webhook endpoints
        logger.info("🔗 Webhook notification sent")
        # Implementation would go here
    
    async def _save_incident(self, incident: QualityIncident) -> None:
        """Save incident to persistent storage"""
        incident_file = self.incidents_dir / f"{incident.incident_id}.json"
        
        incident_data = {
            "incident_id": incident.incident_id,
            "timestamp": incident.timestamp.isoformat(),
            "severity": incident.severity.value,
            "title": incident.title,
            "description": incident.description,
            "affected_datasets": incident.affected_datasets,
            "quality_score": incident.quality_score,
            "failed_checks": incident.failed_checks,
            "drift_detections": incident.drift_detections,
            "metadata": incident.metadata
        }
        
        with open(incident_file, 'w') as f:
            json.dump(incident_data, f, indent=2)
    
    async def _save_response_log(self, response_log: Dict[str, Any]) -> None:
        """Save response log to persistent storage"""
        log_file = self.incidents_dir / f"{response_log['incident_id']}_response.json"
        
        with open(log_file, 'w') as f:
            json.dump(response_log, f, indent=2)
    
    def get_incident_status(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of an incident"""
        if incident_id in self.active_incidents:
            incident = self.active_incidents[incident_id]
            return {
                "incident_id": incident_id,
                "severity": incident.severity.value,
                "status": "active",
                "created_at": incident.timestamp.isoformat(),
                "quality_score": incident.quality_score
            }
        return None
    
    def list_active_incidents(self) -> List[Dict[str, Any]]:
        """List all active incidents"""
        return [
            self.get_incident_status(incident_id)
            for incident_id in self.active_incidents.keys()
        ]
    
    async def resolve_incident(self, incident_id: str, resolution_notes: str = "") -> bool:
        """Mark an incident as resolved"""
        if incident_id in self.active_incidents:
            incident = self.active_incidents.pop(incident_id)
            
            # Save resolution
            resolution_file = self.incidents_dir / f"{incident_id}_resolution.json"
            resolution_data = {
                "incident_id": incident_id,
                "resolved_at": datetime.now().isoformat(),
                "resolution_notes": resolution_notes,
                "duration_minutes": (datetime.now() - incident.timestamp).total_seconds() / 60
            }
            
            with open(resolution_file, 'w') as f:
                json.dump(resolution_data, f, indent=2)
            
            logger.success(f"✅ Incident {incident_id} resolved")
            return True
        
        return False
