import re
import json
import csv
from collections import defaultdict, Counter
from datetime import datetime

class EnhancedSkillExtractor:
    def __init__(self):
        self.skills_database = self._create_comprehensive_database()
        self.skill_variations = self._create_variations_map()
        self.tech_stacks = self._create_tech_stacks()
        
    def _create_comprehensive_database(self):
        """Create comprehensive skill database with 700+ skills"""
        return {
            'programming_languages': [
                'Python', 'JavaScript', 'Java', 'C++', 'C#', 'TypeScript', 'Go', 'Rust', 'Swift', 'Kotlin',
                'PHP', 'Ruby', 'Scala', 'R', 'MATLAB', 'Perl', 'Lua', 'Dart', 'Elixir', 'Clojure',
                'Haskell', 'F#', 'OCaml', 'Erlang', 'Julia', 'Crystal', 'Nim', 'Zig', 'C', 'Assembly',
                'COBOL', 'Fortran', 'Ada', 'Pascal', 'Delphi', 'Visual Basic', 'Objective-C',
                'HTML', 'CSS', 'SCSS', 'SASS', 'Less', 'Stylus', 'CoffeeScript', 'Elm',
                'SQL', 'PL/SQL', 'T-SQL', 'NoSQL', 'GraphQL', 'Bash', 'PowerShell', 'Zsh','Spark'
            ],
            
            'web_frameworks': [
                'React', 'Angular', 'Vue.js', 'Svelte', 'Ember.js', 'Backbone.js', 'Alpine.js',
                'Node.js', 'Express.js', 'Koa.js', 'Fastify', 'NestJS', 'Hapi.js',
                'Django', 'Flask', 'FastAPI', 'Tornado', 'Pyramid', 'Bottle',
                'Spring Boot', 'Spring MVC', 'Struts', 'Play Framework',
                'Ruby on Rails', 'Sinatra', 'Hanami', 'Laravel', 'Symfony', 'CodeIgniter',
                'ASP.NET', 'ASP.NET Core', 'Blazor', 'Next.js', 'Nuxt.js', 'Gatsby',
                'SvelteKit', 'Remix', 'Solid.js', 'Qwik', 'Fresh'
            ],
            
            'databases': [
                'PostgreSQL', 'MySQL', 'SQLite', 'MariaDB', 'Oracle', 'SQL Server',
                'MongoDB', 'CouchDB', 'Amazon DocumentDB', 'Azure Cosmos DB',
                'Redis', 'Amazon DynamoDB', 'Riak', 'Cassandra', 'HBase',
                'Neo4j', 'Amazon Neptune', 'ArangoDB', 'OrientDB', 'JanusGraph',
                'InfluxDB', 'TimescaleDB', 'Prometheus', 'OpenTSDB',
                'Elasticsearch', 'Solr', 'Amazon CloudSearch', 'Algolia',
                'Snowflake', 'Amazon Redshift', 'Google BigQuery', 'Azure Synapse',
                'Firebase', 'Supabase', 'PlanetScale', 'Neon'
            ],
            
            'cloud_platforms': [
                'AWS', 'Microsoft Azure', 'Google Cloud Platform', 'IBM Cloud', 'Oracle Cloud',
                'DigitalOcean', 'Linode', 'Vultr', 'Heroku', 'Vercel', 'Netlify',
                'EC2', 'S3', 'RDS', 'Lambda', 'CloudFormation', 'CloudWatch',
                'Azure Functions', 'Azure Kubernetes Service', 'Azure DevOps',
                'Compute Engine', 'Cloud Storage', 'Cloud SQL', 'Cloud Functions',
                'Cloud Run', 'App Engine', 'BigQuery', 'Pub/Sub'
            ],
            
            'containerization_orchestration': [
                'Docker', 'Kubernetes', 'OpenShift', 'Rancher', 'Docker Swarm',
                'Podman', 'LXC', 'Containerd', 'CRI-O', 'Helm', 'Kustomize',
                'ArgoCD', 'Flux', 'Istio', 'Linkerd', 'Consul', 'Nomad'
            ],
            
            'devops_cicd': [
                'Jenkins', 'GitLab CI', 'GitHub Actions', 'Azure DevOps', 'CircleCI',
                'Travis CI', 'TeamCity', 'Bamboo', 'Drone', 'Spinnaker',
                'Ansible', 'Terraform', 'Pulumi', 'CloudFormation', 'ARM Templates',
                'Puppet', 'Chef', 'SaltStack', 'Vagrant', 'Packer'
            ],
            
            'monitoring_logging': [
                'Prometheus', 'Grafana', 'Nagios', 'Zabbix', 'Datadog', 'New Relic',
                'Splunk', 'ELK Stack', 'Fluentd', 'Logstash', 'Kibana',
                'Jaeger', 'Zipkin', 'OpenTelemetry', 'Sentry', 'Rollbar'
            ],
            
            'version_control': [
                'Git', 'GitHub', 'GitLab', 'Bitbucket', 'SVN', 'Mercurial',
                'Perforce', 'TFS', 'Azure Repos', 'CodeCommit'
            ],
            
            'data_science_ml': [
                'NumPy', 'Pandas', 'Matplotlib', 'Seaborn', 'Plotly', 'Bokeh',
                'TensorFlow', 'PyTorch', 'Keras', 'Scikit-learn', 'XGBoost', 'LightGBM',
                'Apache Spark', 'Hadoop', 'Hive', 'Kafka', 'Airflow', 'Prefect',
                'MLflow', 'Kubeflow', 'SageMaker', 'Azure ML', 'Vertex AI',
                'Jupyter', 'JupyterLab', 'Google Colab', 'Databricks'
            ],
            
            'data_visualization': [
                'Tableau', 'Power BI', 'Qlik Sense', 'Looker', 'D3.js', 'Chart.js',
                'Highcharts', 'Plotly', 'Observable', 'Apache Superset'
            ],
            
            'mobile_development': [
                'React Native', 'Flutter', 'Xamarin', 'Ionic', 'Cordova', 'PhoneGap',
                'Swift', 'Objective-C', 'Kotlin', 'Java', 'Dart',
                'Android Studio', 'Xcode', 'UIKit', 'SwiftUI', 'Jetpack Compose'
            ],
            
            'testing_qa': [
                'Jest', 'Mocha', 'Jasmine', 'Cypress', 'Playwright', 'Puppeteer',
                'Selenium', 'WebDriver', 'Appium', 'TestComplete', 'Katalon',
                'JUnit', 'TestNG', 'NUnit', 'MSTest', 'PyTest', 'RSpec',
                'Postman', 'Insomnia', 'SoapUI', 'REST Assured', 'Karate',
                'LoadRunner', 'JMeter', 'K6', 'Gatling', 'Artillery'
            ],
            
            'cybersecurity': [
                'OWASP', 'NIST', 'ISO 27001', 'SANS', 'MITRE ATT&CK',
                'Nmap', 'Wireshark', 'Metasploit', 'Burp Suite', 'OWASP ZAP',
                'Kali Linux', 'Parrot OS', 'Penetration Testing', 'SIEM',
                'Splunk', 'QRadar', 'ArcSight', 'FortiSIEM',
                'GDPR', 'HIPAA', 'SOX', 'PCI DSS', 'CCPA'
            ],
            
            'business_project_management': [
                'Agile', 'Scrum', 'Kanban', 'Waterfall', 'PRINCE2', 'PMP', 'SAFe',
                'Jira', 'Confluence', 'Trello', 'Asana', 'Monday.com', 'ClickUp',
                'Project Management', 'Business Analysis', 'Requirements Gathering',
                'Technical Writing', 'Leadership', 'Team Management', 'Stakeholder Management'
            ],
            
            'design_ux_ui': [
                'Figma', 'Sketch', 'Adobe XD', 'InVision', 'Principle', 'Framer',
                'Photoshop', 'Illustrator', 'After Effects', 'Canva',
                'User Experience', 'User Interface', 'Wireframing', 'Prototyping',
                'Design Systems', 'Accessibility', 'WCAG', 'Material Design'
            ]
        }
    
    def _create_variations_map(self):
        """Create mapping of skill variations and abbreviations"""
        return {
            'javascript': ['js', 'node.js', 'nodejs', 'ecmascript'],
            'spark': ['pyspark', 'SparkR', 'nodejs', 'ecmascript'],
            'python': ['py'],
            'kubernetes': ['k8s'],
            'docker': ['containerization'],
            'postgresql': ['postgres', 'psql'],
            'amazon web services': ['aws'],
            'google cloud platform': ['gcp', 'google cloud'],
            'microsoft azure': ['azure'],
            'machine learning': ['ml', 'artificial intelligence', 'ai'],
            'deep learning': ['dl', 'neural networks'],
            'natural language processing': ['nlp'],
            'computer vision': ['cv'],
            'continuous integration': ['ci'],
            'continuous deployment': ['cd', 'ci/cd'],
            'devops': ['dev ops'],
            'frontend': ['front-end', 'front end'],
            'backend': ['back-end', 'back end'],
            'fullstack': ['full-stack', 'full stack'],
            'database': ['db'],
            'application programming interface': ['api', 'rest api', 'restful api'],
            'user interface': ['ui'],
            'user experience': ['ux'],
            'search engine optimization': ['seo'],
            'version control': ['git', 'svn'],
            'test driven development': ['tdd'],
            'behavior driven development': ['bdd'],
            'microservices': ['micro services', 'service oriented architecture', 'soa'],
            'serverless': ['faas', 'function as a service'],
            'infrastructure as code': ['iac'],
            'single page application': ['spa'],
            'progressive web app': ['pwa'],
            'representational state transfer': ['rest', 'restful']
        }
    
    def _create_tech_stacks(self):
        """Define common technology stacks"""
        return {
            'MEAN Stack': ['MongoDB', 'Express.js', 'Angular', 'Node.js'],
            'MERN Stack': ['MongoDB', 'Express.js', 'React', 'Node.js'],
            'MEVN Stack': ['MongoDB', 'Express.js', 'Vue.js', 'Node.js'],
            'LAMP Stack': ['Linux', 'Apache', 'MySQL', 'PHP'],
            'LEMP Stack': ['Linux', 'Nginx', 'MySQL', 'PHP'],
            'Django Stack': ['Python', 'Django', 'PostgreSQL'],
            'Rails Stack': ['Ruby', 'Ruby on Rails', 'PostgreSQL'],
            'JAMstack': ['JavaScript', 'APIs', 'Markup'],
            'Serverless Stack': ['AWS Lambda', 'API Gateway', 'DynamoDB'],
            'ELK Stack': ['Elasticsearch', 'Logstash', 'Kibana']
        }
    
    def extract_skills(self, text, include_context=False):
        """
        Extract skills from text with comprehensive matching
        
        Args:
            text (str): Input text to analyze
            include_context (bool): Whether to include context for found skills
            
        Returns:
            dict: Extracted skills organized by category
        """
        text_lower = text.lower()
        found_skills = defaultdict(set)
        skill_contexts = {} if include_context else None
        
        # Create skill-to-category mapping
        skill_to_category = {}
        for category, skills in self.skills_database.items():
            for skill in skills:
                skill_to_category[skill.lower()] = category
        
        # Method 1: Direct skill matching
        for category, skills in self.skills_database.items():
            for skill in skills:
                pattern = r'\b' + re.escape(skill.lower()) + r'\b'
                matches = list(re.finditer(pattern, text_lower))
                
                if matches:
                    found_skills[category].add(skill)
                    
                    if include_context and matches:
                        # Get context for first match
                        match = matches[0]
                        start = max(0, match.start() - 30)
                        end = min(len(text), match.end() + 30)
                        skill_contexts[skill] = text[start:end].strip()
        
        # Method 2: Skill variations and abbreviations
        for main_skill, variations in self.skill_variations.items():
            for variation in variations:
                pattern = r'\b' + re.escape(variation) + r'\b'
                if re.search(pattern, text_lower):
                    # Find matching skill in database
                    for category, skills in self.skills_database.items():
                        matching_skills = [s for s in skills if main_skill in s.lower()]
                        for skill in matching_skills:
                            found_skills[category].add(skill)
        
        # Method 3: Technology stack detection
        for stack_name, stack_techs in self.tech_stacks.items():
            stack_pattern = stack_name.lower().replace(' ', r'\s*')
            if re.search(stack_pattern, text_lower):
                for tech in stack_techs:
                    for category, skills in self.skills_database.items():
                        if tech in skills:
                            found_skills[category].add(tech)
        
        # Convert sets to sorted lists
        result = {cat: sorted(list(skills)) for cat, skills in found_skills.items()}
        
        if include_context:
            return result, skill_contexts
        return result
    
    def analyze_skill_profile(self, extracted_skills):
        """Analyze extracted skills and provide insights"""
        
        total_skills = sum(len(skills) for skills in extracted_skills.values())
        
        # Skill level assessment
        if total_skills >= 50:
            level = "Expert/Architect"
        elif total_skills >= 35:
            level = "Senior"
        elif total_skills >= 20:
            level = "Mid-Senior"
        elif total_skills >= 10:
            level = "Mid-Level"
        else:
            level = "Junior/Entry"
        
        # Category analysis
        category_counts = {cat: len(skills) for cat, skills in extracted_skills.items()}
        top_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Determine role suggestions based on skill distribution
        role_suggestions = self._suggest_roles(category_counts)
        
        # Calculate coverage
        total_possible = sum(len(skills) for skills in self.skills_database.values())
        coverage_percent = (total_skills / total_possible) * 100
        
        return {
            'total_skills': total_skills,
            'estimated_level': level,
            'top_categories': top_categories[:5],
            'diversity_score': len(extracted_skills),
            'coverage_percent': round(coverage_percent, 2),
            'role_suggestions': role_suggestions
        }
    
    def _suggest_roles(self, category_counts):
        """Suggest potential roles based on skill distribution"""
        
        roles = []
        
        # Define role patterns
        if (category_counts.get('web_frameworks', 0) >= 3 and 
            category_counts.get('programming_languages', 0) >= 2):
            if category_counts.get('data_science_ml', 0) >= 2:
                roles.append("Full Stack Data Engineer")
            else:
                roles.append("Full Stack Developer")
        
        if category_counts.get('data_science_ml', 0) >= 4:
            roles.append("Data Scientist/ML Engineer")
        
        if (category_counts.get('devops_cicd', 0) >= 3 and 
            category_counts.get('containerization_orchestration', 0) >= 2):
            roles.append("DevOps Engineer")
        
        if (category_counts.get('cloud_platforms', 0) >= 4 and 
            category_counts.get('containerization_orchestration', 0) >= 2):
            roles.append("Cloud Architect")
        
        if category_counts.get('mobile_development', 0) >= 3:
            roles.append("Mobile Developer")
        
        if category_counts.get('cybersecurity', 0) >= 4:
            roles.append("Security Engineer")
        
        if category_counts.get('business_project_management', 0) >= 3:
            roles.append("Technical Project Manager")
        
        return roles if roles else ["Software Developer"]
    
    def get_trending_recommendations(self, extracted_skills, year=2024):
        """Get trending technology recommendations"""
        
        all_found_skills = [skill.lower() for skills in extracted_skills.values() for skill in skills]
        
        trending_2024 = {
            'AI/ML': ['ChatGPT', 'LangChain', 'Hugging Face', 'OpenAI API', 'Stable Diffusion'],
            'Web': ['Astro', 'Remix', 'SvelteKit', 'Solid.js', 'Qwik'],
            'Backend': ['Bun', 'Deno', 'Tauri', 'tRPC', 'Prisma'],
            'Mobile': ['Expo', 'Tamagui', 'NativeScript', 'Capacitor'],
            'DevOps': ['Pulumi', 'Crossplane', 'ArgoCD', 'Tekton', 'Backstage'],
            'Database': ['Supabase', 'PlanetScale', 'Neon', 'Turso', 'EdgeDB'],
            'Languages': ['Rust', 'Go', 'Zig', 'TypeScript', 'WebAssembly']
        }
        
        recommendations = {}
        for category, techs in trending_2024.items():
            missing = [tech for tech in techs if tech.lower() not in all_found_skills]
            if missing:
                recommendations[category] = missing[:3]  # Top 3 per category
        
        return recommendations
    
    def export_to_json(self, extracted_skills, analysis, filename=None):
        """Export results to JSON file"""
        
        if filename is None:
            filename = f"skills_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'skills': extracted_skills,
            'analysis': analysis,
            'metadata': {
                'extractor_version': '2.0',
                'total_database_skills': sum(len(skills) for skills in self.skills_database.values())
            }
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        return filename
    
    def export_to_csv(self, extracted_skills, filename=None):
        """Export skills to CSV file"""
        
        if filename is None:
            filename = f"skills_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Category', 'Skill', 'Skill_Count_In_Category'])
            
            for category, skills in extracted_skills.items():
                for skill in skills:
                    writer.writerow([category.replace('_', ' ').title(), skill, len(skills)])
        
        return filename

    def export_to_json(self, extracted_skills, analysis, filename=None):
        """Export results to JSON file"""
        
        if filename is None:
            filename = f"skills_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'skills': extracted_skills,
            'analysis': analysis,
            'metadata': {
                'extractor_version': '2.0',
                'total_database_skills': sum(len(skills) for skills in self.skills_database.values())
            }
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        return filename
    
    def export_to_csv(self, extracted_skills, filename=None):
        """Export skills to CSV file"""
        
        if filename is None:
            filename = f"skills_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Category', 'Skill', 'Skill_Count_In_Category'])
            
            for category, skills in extracted_skills.items():
                for skill in skills:
                    writer.writerow([category.replace('_', ' ').title(), skill, len(skills)])
        
        return filename

# Example usage and testing

extractor = EnhancedSkillExtractor()
    
    # Test with sample resume
sample_resume = """
    About the job
Data Analytics Engineer - Mercedes-Benz Vans



Be the change. Join our team. 

Becoming part of Mercedes-Benz means finding your individual role and workspace to unleash your talents to the fullest. It means becoming your best self in a global automotive company striving towards sustainable luxury. Empowered by visionary colleagues who share the same pioneering spirit.


At Mercedes-Benz Vans, we’re a balance of passion, professionalism, and approachability. Our dynamic network across Australia and New Zealand has an established culture of support and collaboration, ensuring our service is exceptional and our customers’ expectations are exceeded.
The opportunity


We have a very exciting opportunity for a passionate individual to join our Vans Data team! In this permanent role, you will be responsible for designing, building, and optimising data pipelines and analytics solutions to enable fast, accurate, and actionable insights across the Mercedes-Benz Vans business. The ideal candidate will have extensive experience with finding data solutions and modelling large data sets, and creating details reports and presentations to support business KPIs.
Our team is based in Mulgrave, Melbourne, and this hybrid role has great variety and flexibility, allowing a mix of working from our newly refurbished offices and home.


Key duties & responsibilities:

Data Management: Oversee data sources and ingestion processes, ensuring seamless integration and collaboration with stakeholders.
Solution Development: Design, maintain, and troubleshoot data solutions using SQL, Python, Spark, Databricks, and Power BI to meet business needs.
Analytics & Insights: Provide actionable insights by analyzing data to solve complex business problems and support strategic decision-making.
Collaboration: Partner with analysts and business stakeholders to deliver effective data solutions and foster interdepartmental relationships.
Innovation & Automation: Explore ML/AI technologies to enhance data products and automate reporting processes, driving efficiency and innovation.
Strategy & Reporting: Develop Power BI reports and dashboards that communicate key metrics and support business objectives.


What you will need to be successful:

Education: Qualification in Information Technology, Computer Science, Engineering, Mathematics, or related discipline (STEM related)
Experience: Minimum of 2 years in a multinational organization, with a focus on data collection and analysis.
Extensive experience in data collection, sorting and modelling of large data sets and creating detailed reports and presentations highly regarded, and experience with data mining, statistical methods, machine-learning is preferred.
Technical Skills: Proficiency in Python, SQL, and Spark programming languages, along with experience in Power BI and data visualisation tools.
Tools Knowledge: Familiarity with Azure, Databricks, and cloud computing platforms.
Soft Skills: Strong problem-solving abilities, attention to detail, and excellent interpersonal and communication skills.
    """

skills = extractor.extract_skills(sample_resume)
print(skills)

suggest=extractor.analyze_skill_profile(skills)
print(suggest)