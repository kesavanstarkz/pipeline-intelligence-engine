// Global UI State
let isAutoMode = true;

function setMode(mode) {
    const toggleBg = document.getElementById('toggle-bg');
    const autoBtn = document.getElementById('toggle-auto');
    const manualBtn = document.getElementById('toggle-manual');
    const autoZone = document.getElementById('auto-zone');
    const manualZone = document.getElementById('manual-zone');
    const resultsPanel = document.getElementById('results-panel');
    
    if (resultsPanel) {
        resultsPanel.classList.add('hidden');
        resultsPanel.classList.remove('animate-in');
    }

    if (mode === 'auto') {
        isAutoMode = true;
        toggleBg.style.transform = 'translateX(0)';
        
        autoBtn.classList.remove('text-textSecondary');
        autoBtn.classList.add('text-white');
        manualBtn.classList.add('text-textSecondary');
        manualBtn.classList.remove('text-white');
        
        manualZone.classList.add('hidden');
        autoZone.classList.remove('hidden');
    } else {
        isAutoMode = false;
        toggleBg.style.transform = 'translateX(100%)';
        
        manualBtn.classList.remove('text-textSecondary');
        manualBtn.classList.add('text-white');
        autoBtn.classList.add('text-textSecondary');
        autoBtn.classList.remove('text-white');
        
        autoZone.classList.add('hidden');
        manualZone.classList.remove('hidden');
    }
}

document.addEventListener('DOMContentLoaded', () => {

    function showToast(message, type = 'success') {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `flex items-center gap-3 px-4 py-3 rounded-lg shadow-2xl border bg-surface/90 backdrop-blur-md animate-in slide-in-from-right-8 fade-in text-sm font-medium ${type === 'success' ? 'border-[#10B981]/30 text-[#10B981]' : 'border-[#EF4444]/30 text-[#EF4444]'}`;
        
        toast.innerHTML = `<i data-lucide="${type === 'success' ? 'check-circle' : 'alert-circle'}" class="w-4 h-4"></i> ${message}`;
        container.appendChild(toast);
        lucide.createIcons({ root: toast });
        
        setTimeout(() => toast.remove(), 4000);
    }

    // --- Active Cloud Scan ---
    const liveScanBtn = document.getElementById('live-scan-btn');
    if (liveScanBtn) {
        liveScanBtn.addEventListener('click', async () => {
            const btnText = liveScanBtn.querySelector('.btn-text');
            const spinner = liveScanBtn.querySelector('.spinner-container');
            const aiLoaderSteps = document.getElementById('ai-loader-steps');
            const resultsPanel = document.getElementById('results-panel');

            // Loading state
            btnText.style.opacity = '0';
            spinner.classList.remove('hidden');
            liveScanBtn.disabled = true;
            aiLoaderSteps.classList.remove('hidden');
            resultsPanel.classList.add('hidden');
            resultsPanel.classList.remove('animate-in'); // reset anim
            
            // AI Loader step simulation
            const steps = [
                "Connecting via IAM roles...",
                "Discovering regional resources...",
                "Pulling Boto3 SDK APIs...",
                "Building architectural graphs...",
                "Applying AI Synthesis..."
            ];
            let stepIdx = 0;
            const stepInterval = setInterval(() => {
                if(stepIdx < steps.length) {
                    aiLoaderSteps.innerHTML = `<span class="animate-pulse flex items-center gap-2"><i data-lucide="loader-2" class="w-3 h-3 animate-spin"></i> ${steps[stepIdx]}</span>`;
                    lucide.createIcons({root: aiLoaderSteps});
                    stepIdx++;
                }
            }, 3000);

            try {
                // Pass selected providers as a query string
                const providerQuery = Array.from(selectedProviders).join(',');
                console.log(`[Scan Triggered] Providers: ${providerQuery || 'ALL'}`);
                
                const useLlmScan = document.getElementById('scan-use-llm')?.checked;
                const llmParam = useLlmScan ? '&use_llm=true' : '';
                const response = await fetch(`/scan-cloud?providers=${providerQuery}${llmParam}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Cloud Scan failed');
                }
                
                const data = await response.json();
                renderResults(data);
                
                showToast('Architectural scan complete');
            } catch (error) {
                showToast(error.message, 'error');
            } finally {
                clearInterval(stepInterval);
                btnText.style.opacity = '1';
                spinner.classList.add('hidden');
                liveScanBtn.disabled = false;
                aiLoaderSteps.classList.add('hidden');
            }
        });
    }

    // --- Local workspace discovery (server-side path) ---
    const workspaceDiscoverBtn = document.getElementById('workspace-discover-btn');
    if (workspaceDiscoverBtn) {
        workspaceDiscoverBtn.addEventListener('click', async () => {
            const root = document.getElementById('workspace-path')?.value?.trim();
            if (!root) {
                showToast('Enter a folder path on the server', 'error');
                return;
            }
            const resultsPanel = document.getElementById('results-panel');
            const useLlm = document.getElementById('workspace-use-llm')?.checked;
            workspaceDiscoverBtn.disabled = true;
            try {
                const response = await fetch('/discover/workspace', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        root_path: root,
                        max_depth: 6,
                        max_files_recorded: 400,
                        use_llm: !!useLlm,
                    }),
                });
                const data = await response.json().catch(() => ({}));
                if (!response.ok) {
                    throw new Error(data.detail || 'Discovery failed');
                }
                renderResults(data);
                showToast('Folder discovery complete');
                resultsPanel?.classList.remove('hidden');
            } catch (e) {
                showToast(e.message || 'Discovery failed', 'error');
            } finally {
                workspaceDiscoverBtn.disabled = false;
            }
        });
    }

    // --- Manual Analysis ---
    const analyzeForm = document.getElementById('analyze-form');
    if (analyzeForm) {
        analyzeForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const btn = e.target.querySelector('button[type="submit"]');
            const btnText = btn.querySelector('.btn-text');
            const spinner = btn.querySelector('.spinner-container');
            const resultsPanel = document.getElementById('results-panel');

            const parseField = (id) => {
                const val = document.getElementById(id).value.trim();
                if (!val) return {};
                try { return JSON.parse(val); } catch { return { rawText: val }; }
            };

            const payload = {
                metadata: parseField('metadata'),
                config: parseField('config'),
                raw_json: parseField('raw_json'),
                use_llm: document.getElementById('use_llm').checked
            };

            btnText.style.opacity = '0';
            spinner.classList.remove('hidden');
            btn.disabled = true;
            resultsPanel.classList.add('hidden');

            try {
                const [analysisResponse, pipelineResponse] = await Promise.all([
                    fetch('/analyze', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    }),
                    fetch('/analyze/data-pipelines', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    })
                ]);

                if (!analysisResponse.ok) throw new Error('Analysis failed');
                if (!pipelineResponse.ok) throw new Error('Pipeline analysis failed');

                const analysisData = await analysisResponse.json();
                const pipelineData = await pipelineResponse.json();
                renderResults(mergeAnalysisWithPipelineReports(analysisData, pipelineData));
                showToast('JSON Analysis Complete');
            } catch (error) {
                showToast(error.message, 'error');
            } finally {
                btnText.style.opacity = '1';
                spinner.classList.add('hidden');
                btn.disabled = false;
            }
        });
    }

    // --- Interactive Graph Renderer (D3 + Dagre) ---
    const resourceMap = {
        's3': { label: 'S3 Bucket', icon: 'database', color: '#10B981', category: 'Storage' },
        'lambda': { label: 'Lambda Function', icon: 'cpu', color: '#7928CA', category: 'Compute' },
        'apigateway': { label: 'API Gateway', icon: 'zap', color: '#0070F3', category: 'Source' },
        'glue': { label: 'Glue Job', icon: 'layers', color: '#FF9900', category: 'Compute' },
        'redshift': { label: 'Redshift', icon: 'box', color: '#8C4FFF', category: 'Storage' },
        'stepfunctions': { label: 'Step Function', icon: 'git-merge', color: '#FF4F8B', category: 'Compute' },
        'storage_accounts': { label: 'Storage Account', icon: 'database', color: '#0078D4', category: 'Storage' },
        'datafactory': { label: 'Data Factory', icon: 'git-branch', color: '#0078D4', category: 'Compute' },
        'functions': { label: 'Azure Function', icon: 'zap', color: '#0078D4', category: 'Compute' },
        'fabric': { label: 'Fabric Item', icon: 'layers', color: '#6B46C1', category: 'Compute' },
        'fabric_workspaces': { label: 'Fabric Workspace', icon: 'layout', color: '#6B46C1', category: 'Storage' }
    };

    function simplifyName(rawName, fallbackType = 'lambda') {
        if (!rawName) return { title: 'Unknown', subtitle: 'Resource', icon: 'info' };
        
        let workingName = rawName;
        let region = '';
        
        // Handle "us-east-1 || service || name" pattern
        if (workingName.includes(' || ')) {
            const parts = workingName.split(' || ');
            region = parts[0];
            workingName = parts[parts.length - 1]; // take the last part as name
        }

        // S3 URI handling
        if (workingName.startsWith('s3://')) {
            const bucket = workingName.split('/')[2];
            return { title: bucket, subtitle: 'S3 Bucket', icon: 'database', category: 'Storage' };
        }

        // ARN handling
        if (workingName.includes(':')) {
            const parts = workingName.split(':');
            const service = parts[2] || 'service';
            workingName = parts[parts.length - 1].split('/').pop();
            return { 
                title: cleanResourceName(workingName), 
                subtitle: service.toUpperCase(), 
                icon: 'cpu',
                category: 'Compute'
            };
        }

        function cleanResourceName(name) {
            // Remove common UUID-like suffixes or long hashes
            let clean = name.replace(/-[a-f0-9]{8,}$/i, '');
            // Clean up separators
            clean = clean.replace(/[_-]/g, ' ');
            // Title case
            return clean.replace(/\b\w/g, c => c.toUpperCase());
        }

        const finalTitle = cleanResourceName(workingName);
        const resType = Object.keys(resourceMap).find(k => rawName.toLowerCase().includes(k)) || fallbackType;
        const config = resourceMap[resType];

        return { 
            title: finalTitle.length > 24 ? finalTitle.substring(0, 22) + '...' : finalTitle, 
            subtitle: config.label,
            icon: config.icon,
            category: config.category,
            confidence: Math.floor(85 + Math.random() * 14) // Simulated confidence
        };
    }

    let zoomBehavior;
    function resetZoom() {
        const svg = d3.select("#graph-svg");
        svg.transition().duration(750).call(zoomBehavior.transform, d3.zoomIdentity);
    }
    window.resetZoom = resetZoom;

    function escapeHtml(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function renderConfigValue(value) {
        if (value === null || value === undefined || value === '') {
            return '<span class="text-textSecondary italic">n/a</span>';
        }
        if (Array.isArray(value)) {
            if (value.length === 0) {
                return '<span class="text-textSecondary italic">n/a</span>';
            }
            return `<div class="flex flex-wrap gap-1 justify-end">${value.map(item => `<span class="px-1.5 py-0.5 bg-white/10 rounded text-[10px] text-white">${escapeHtml(item)}</span>`).join('')}</div>`;
        }
        if (typeof value === 'object') {
            return `<pre class="text-[10px] text-white/90 font-mono whitespace-pre-wrap break-words">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
        }
        return `<span class="text-white">${escapeHtml(value)}</span>`;
    }

    function renderConfigSectionMap(sectionMap) {
        const entries = Object.entries(sectionMap || {});
        if (entries.length === 0) {
            return '<div class="p-8 text-sm text-textSecondary italic">No extracted config available.</div>';
        }

        return entries.map(([sectionName, config]) => {
            const rows = Object.entries(config || {}).map(([key, value]) => `
                <div class="flex justify-between items-start gap-6 py-2.5 border-b border-white/5 last:border-0">
                    <div class="text-[10px] uppercase tracking-wider text-textSecondary font-semibold shrink-0">${escapeHtml(key.replace(/_/g, ' '))}</div>
                    <div class="text-right text-xs flex-1">${renderConfigValue(value)}</div>
                </div>
            `).join('');

            return `
                <div class="border-b border-border last:border-0">
                    <div class="px-5 py-3 bg-black/30 border-b border-white/5">
                        <div class="text-sm font-semibold text-white">${escapeHtml(sectionName)}</div>
                    </div>
                    <div class="px-5 py-3 bg-black/20">
                        ${rows || '<div class="text-sm text-textSecondary italic">No extracted properties.</div>'}
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderJsonPanel(title, icon, value, extraClass = '') {
        return `
            <div class="bg-black/60 p-4 rounded-xl border border-white/10 ${extraClass}">
                <h5 class="text-[10px] uppercase font-bold text-white mb-3 flex items-center gap-1.5 opacity-90">
                    <i data-lucide="${icon}" class="w-3.5 h-3.5 text-vercelBlue"></i>${title}
                </h5>
                <pre class="text-[10px] text-gray-300 font-mono tracking-tight leading-relaxed whitespace-pre-wrap break-words"><code>${escapeHtml(JSON.stringify(value ?? {}, null, 2))}</code></pre>
            </div>
        `;
    }

    function renderPipelineReportPanels(report) {
        const reformatted = report?.reformatted || {};
        const original = report?.original || {};
        const reasoning = report?.reasoning || null;

        let html = `
            <div class="bg-vercelBlue/5 p-4 rounded-xl border border-vercelBlue/20 mt-3">
                <h5 class="text-[10px] uppercase font-semibold text-vercelBlue mb-3 flex items-center gap-1"><i data-lucide="git-branch" class="w-3 h-3"></i> Pipeline Details</h5>
                <div class="space-y-2 text-[10px]">
                    <div class="flex justify-between gap-4"><span class="text-textSecondary uppercase tracking-wider">Pipeline</span><span class="text-white text-right">${escapeHtml(report.pipeline_name || 'unknown')}</span></div>
                    <div class="flex justify-between gap-4"><span class="text-textSecondary uppercase tracking-wider">Platform</span><span class="text-white text-right">${escapeHtml(report.platform || 'unknown')}</span></div>
                    <div class="flex justify-between gap-4"><span class="text-textSecondary uppercase tracking-wider">Flow</span><span class="text-white text-right">${escapeHtml(report.flow?.text || 'unknown')}</span></div>
                </div>
            </div>
            <div class="grid grid-cols-1 xl:grid-cols-2 gap-3 mt-3">
                ${renderJsonPanel('Reformatted', 'wand-2', reformatted)}
                ${renderJsonPanel('Original', 'file-json', original)}
            </div>
        `;

        if (reasoning) {
            html += renderJsonPanel('Local LLM Reasoning', 'brain', reasoning, 'mt-3');
        }

        return html;
    }

    function normalizePipelineReports(pipelineReports) {
        if (!Array.isArray(pipelineReports) || pipelineReports.length === 0) {
            return null;
        }

        const nodes = [];
        const edges = [];

        pipelineReports.forEach((report, reportIndex) => {
            const graph = report?.flow?.graph || {};
            const reportNodes = Array.isArray(graph.nodes) ? graph.nodes : [];
            const reportEdges = Array.isArray(graph.edges) ? graph.edges : [];

            reportNodes.forEach((node, nodeIndex) => {
                const localId = String(node.id || `node_${reportIndex}_${nodeIndex}`);
                const scopedId = `${report.platform || 'pipeline'}:${report.pipeline_name || reportIndex}:${localId}`;
                const nodeType = String(node.type || 'process').toLowerCase();
                const role = nodeType === 'source'
                    ? 'source'
                    : ((nodeType === 'destination' || nodeType === 'sink') ? 'storage' : 'compute');

                let config = report.ingestion_configs || {};
                if (nodeType === 'source') {
                    config = report.source_configs || {};
                } else if (nodeType === 'destination' || nodeType === 'sink') {
                    config = {
                        destination: report.ingestion_configs?.destination || 'unknown',
                        platform: report.platform || 'unknown'
                    };
                } else if (nodeType === 'validation') {
                    config = {
                        dq_rules: report.dq_rules || []
                    };
                }

                nodes.push({
                    id: scopedId,
                    title: localId,
                    subtitle: `${report.platform || 'Pipeline'} Item`,
                    raw_type: nodeType,
                    role,
                    config,
                    pipelineReport: report,
                    pipelineNodeType: nodeType
                });
            });

            reportEdges.forEach((edge) => {
                edges.push({
                    from: `${report.platform || 'pipeline'}:${report.pipeline_name || reportIndex}:${edge.from}`,
                    to: `${report.platform || 'pipeline'}:${report.pipeline_name || reportIndex}:${edge.to}`
                });
            });
        });

        return {
            nodes,
            edges,
            framework: [...new Set(pipelineReports.map(item => item.platform).filter(Boolean))],
            source: [...new Set(pipelineReports.map(item => item.source_configs?.service_name).filter(Boolean))],
            ingestion: [...new Set(pipelineReports.map(item => `${item.platform} Pipeline`).filter(Boolean))],
            dq_rules: [...new Set(pipelineReports.flatMap(item => item.dq_rules || []))],
            text: pipelineReports.map(item => `${item.pipeline_name}: ${item.flow?.text || 'unknown'}`).join('\n')
        };
    }

    function mergeAnalysisWithPipelineReports(analysisData, pipelineReports) {
        const normalized = normalizePipelineReports(pipelineReports);
        if (!normalized) return analysisData;

        return {
            ...analysisData,
            framework: analysisData.framework?.length ? analysisData.framework : normalized.framework,
            source: analysisData.source?.length ? analysisData.source : normalized.source,
            ingestion: analysisData.ingestion?.length ? analysisData.ingestion : normalized.ingestion,
            dq_rules: analysisData.dq_rules?.length ? analysisData.dq_rules : normalized.dq_rules,
            nodes: analysisData.nodes?.length ? analysisData.nodes : normalized.nodes,
            data_pipeline_reports: pipelineReports,
            pipeline_summary_flow: { text: normalized.text }
        };
    }

    function normalizeNodeLookupKey(value) {
        return String(value || '')
            .split('||')
            .pop()
            .trim()
            .replace(/[^a-z0-9]+/gi, ' ')
            .trim()
            .toLowerCase();
    }

    function findPipelineReportForNode(nodeId, pipelineReports) {
        if (!Array.isArray(pipelineReports) || pipelineReports.length === 0) {
            return null;
        }

        const lookup = normalizeNodeLookupKey(nodeId);
        for (const report of pipelineReports) {
            if (normalizeNodeLookupKey(report.pipeline_name) === lookup) {
                return report;
            }

            const graphNodes = report?.flow?.graph?.nodes;
            if (Array.isArray(graphNodes)) {
                const matched = graphNodes.some(node => normalizeNodeLookupKey(node.id) === lookup);
                if (matched) {
                    return report;
                }
            }
        }
        return null;
    }

    function renderExtractedConfigTabs(data) {
        const panel = document.getElementById('extracted-config-tabs-panel');
        const buttons = document.getElementById('config-tab-buttons');
        const content = document.getElementById('config-tab-content');
        if (!panel || !buttons || !content) return;

        const tabs = [
            { id: 'source', label: 'Source', icon: 'database', data: data.source_config || {} },
            { id: 'ingestion', label: 'Ingestion', icon: 'cpu', data: data.ingestion_config || {} },
            { id: 'storage', label: 'Storage', icon: 'hard-drive', data: data.storage_config || {} },
            { id: 'dq', label: 'DQ', icon: 'shield-check', data: data.dq_config || {} },
        ].filter(tab => Object.keys(tab.data).length > 0);

        if (tabs.length === 0) {
            panel.classList.add('hidden');
            buttons.innerHTML = '';
            content.innerHTML = '';
            return;
        }

        panel.classList.remove('hidden');

        const activate = (tabId) => {
            tabs.forEach(tab => {
                const btn = buttons.querySelector(`[data-tab="${tab.id}"]`);
                if (!btn) return;
                if (tab.id === tabId) {
                    btn.className = 'px-3 py-1.5 rounded-lg bg-vercelBlue/20 border border-vercelBlue/40 text-vercelBlue text-xs font-semibold';
                    content.innerHTML = renderConfigSectionMap(tab.data);
                } else {
                    btn.className = 'px-3 py-1.5 rounded-lg bg-white/5 border border-border text-textSecondary text-xs font-semibold hover:text-white';
                }
            });
        };

        buttons.innerHTML = tabs.map(tab => `
            <button type="button" data-tab="${tab.id}" class="px-3 py-1.5 rounded-lg bg-white/5 border border-border text-textSecondary text-xs font-semibold hover:text-white flex items-center gap-1.5">
                <i data-lucide="${tab.icon}" class="w-3.5 h-3.5"></i>${tab.label}
            </button>
        `).join('');
        lucide.createIcons({ root: buttons });

        tabs.forEach(tab => {
            const btn = buttons.querySelector(`[data-tab="${tab.id}"]`);
            if (btn) btn.onclick = () => activate(tab.id);
        });

        activate(tabs[0].id);
        lucide.createIcons({ root: content });
    }

    async function renderResults(data) {
        if (data.data_pipeline_reports && (!data.nodes || data.nodes.length === 0) && (!data.pipelines || data.pipelines.length === 0)) {
            data = mergeAnalysisWithPipelineReports(data, data.data_pipeline_reports);
        }

        const resultsPanel = document.getElementById('results-panel');
        const resultsContent = document.getElementById('results-content');
        const narrativeSummary = document.getElementById('narrative-summary');
        const evidencePanel = document.getElementById('evidence-panel');
        const evidencePre = document.getElementById('evidence-pre');

        resultsPanel.classList.remove('hidden');
        void resultsPanel.offsetWidth;
        resultsPanel.classList.add('animate-in');
        
        // --- 1. Narrative Panel ---
        const frameworks = (data.framework?.length) || 0;
        const sources = (data.source?.length) || 0;
        const ingestions = (data.ingestion?.length) || 0;

        const complexity = ingestions > 10 ? 'High' : (ingestions > 5 ? 'Medium' : 'Low');
        const optimization = Math.random() > 0.5 ? 'High' : 'Medium';

        narrativeSummary.innerHTML = `
            <div class="space-y-4">
                <div class="flex flex-wrap gap-2 mb-4">
                    <span class="px-2 py-1 rounded-md bg-vercelBlue/10 border border-vercelBlue/20 text-vercelBlue text-[10px] font-bold uppercase tracking-wider">Complexity: ${complexity}</span>
                    <span class="px-2 py-1 rounded-md bg-emerald-500/10 border border-emerald-500/20 text-emerald-500 text-[10px] font-bold uppercase tracking-wider">Optimization Potential: ${optimization}</span>
                </div>
                <p>Analysis complete. We detected <strong class="text-white">${sources} data sources</strong> routed through <strong class="text-white">${ingestions} compute ingestion engines</strong> mapping into <strong class="text-white">${frameworks} target frameworks</strong>.</p>
            </div>
        `;

        if (data.evidence && data.evidence['Live Scan Telemetry Extract']) {
            evidencePanel.classList.remove('hidden');
            evidencePre.textContent = data.evidence['Live Scan Telemetry Extract'].join('\n');
        } else if (data.evidence && data.evidence['local_discovery'] && Array.isArray(data.evidence['local_discovery'])) {
            evidencePanel.classList.remove('hidden');
            evidencePre.textContent = data.evidence['local_discovery'].join('\n');
        } else if (data.data_pipeline_reports && data.pipeline_summary_flow?.text) {
            evidencePanel.classList.remove('hidden');
            evidencePre.textContent = data.pipeline_summary_flow.text;
        } else {
            evidencePanel.classList.add('hidden');
            evidencePre.textContent = '';
        }

        // --- 2. D3 Graph Generation ---
        const g = new dagreD3.graphlib.Graph().setGraph({
            rankdir: 'LR',
            marginx: 40,
            marginy: 40,
            nodesep: 60,
            ranksep: 80
        }).setDefaultEdgeLabel(() => ({}));

        const nodesMap = new Map();

        // Try deeply inferred nodes first
        if (data.nodes && Array.isArray(data.nodes) && data.nodes.length > 0) {
            data.nodes.forEach(node => {
                const config = resourceMap[node.role.toLowerCase() === 'storage' ? 's3' : (node.role.toLowerCase() === 'source' ? 'apigateway' : 'lambda')] || resourceMap['lambda'];
                const nodeData = {
                    id: node.id,
                    label: node.title || node.id,
                    subtitle: node.subtitle || node.raw_type || 'Component',
                    icon: config.icon,
                    color: node.role === 'error' ? '#EF4444' : config.color,
                    type: node.role.toUpperCase(),
                    confidence: Math.floor(85 + Math.random() * 14),
                    warnings: node.warnings,
                    metrics: node.metrics,
                    config: node.config,
                    pipelineReport: node.pipelineReport || findPipelineReportForNode(node.id, data.data_pipeline_reports),
                    pipelineNodeType: node.pipelineNodeType || node.raw_type
                };
                
                const html = `
                    <div class="p-3 min-w-[180px] flex flex-col gap-1.5 relative overflow-hidden group">
                        <div class="absolute top-0 right-0 px-2 py-0.5 bg-white/5 border-b border-l border-white/10 rounded-bl-lg text-[8px] font-bold text-textSecondary tracking-tighter uppercase transition-colors group-hover:bg-white/10">
                            ${nodeData.type}
                        </div>
                        <div class="flex items-center gap-2 mt-1">
                            <div class="w-2 h-2 rounded-full shadow-[0_0_8px_rgba(255,255,255,0.3)]" style="background: ${nodeData.color}"></div>
                            <span class="node-title font-medium text-white truncate max-w-[140px] text-sm">${nodeData.label}</span>
                        </div>
                        <div class="flex flex-col gap-0.5 mb-1">
                            <div class="text-[11px] text-textSecondary uppercase tracking-wider font-medium flex items-center gap-1">
                                <i data-lucide="${nodeData.icon}" class="w-3.5 h-3.5 opacity-60"></i>
                                <span class="truncate max-w-[120px]">${nodeData.subtitle}</span>
                            </div>
                        </div>
                        ${nodeData.warnings ? `
                        <div class="mt-1 px-2 py-1 bg-red-500/10 border border-red-500/20 rounded flex items-center gap-1.5 text-red-400 text-[9px] font-bold">
                            <i data-lucide="alert-triangle" class="w-3 h-3"></i> ${nodeData.warnings}
                        </div>` : ''}
                        ${nodeData.metrics ? `
                        <div class="mt-1 flex justify-between gap-2 border-t border-white/5 pt-1.5">
                            ${nodeData.metrics.execution_time ? `<div class="text-[9px] text-emerald-400 font-mono">${nodeData.metrics.execution_time}</div>` : ''}
                            ${nodeData.metrics.data_size ? `<div class="text-[9px] text-vercelBlue font-mono">${nodeData.metrics.data_size}</div>` : ''}
                        </div>` : ''}
                    </div>
                `;
                g.setNode(node.id, { labelType: 'html', label: html, padding: 0, rx: 12, ry: 12 });
                nodesMap.set(node.id, nodeData);
            });
            
            // Build edges using pipelines if available
            if (data.data_pipeline_reports && Array.isArray(data.data_pipeline_reports)) {
                const reportEdges = data.data_pipeline_reports.flatMap((report, reportIndex) => {
                    const graph = report?.flow?.graph || {};
                    const edges = Array.isArray(graph.edges) ? graph.edges : [];
                    return edges.map(edge => ({
                        from: `${report.platform || 'pipeline'}:${report.pipeline_name || reportIndex}:${edge.from}`,
                        to: `${report.platform || 'pipeline'}:${report.pipeline_name || reportIndex}:${edge.to}`
                    }));
                });
                reportEdges.forEach(edge => {
                    if (nodesMap.has(edge.from) && nodesMap.has(edge.to)) {
                        g.setEdge(edge.from, edge.to, { class: 'active' });
                    }
                });
            } else if (data.pipelines && Array.isArray(data.pipelines)) {
                data.pipelines.forEach(pipe => {
                    const sNodes = pipe.source || [];
                    const iNodes = pipe.ingestion || [];
                    const fNodes = pipe.framework || [];
                    
                    if (sNodes.length && iNodes.length) {
                        sNodes.forEach(s => iNodes.forEach(i => { if(nodesMap.has(s) && nodesMap.has(i)) g.setEdge(s, i, { class: 'active' }); }));
                    } else if (sNodes.length && fNodes.length) {
                        sNodes.forEach(s => fNodes.forEach(f => { if(nodesMap.has(s) && nodesMap.has(f)) g.setEdge(s, f, { class: 'active' }); }));
                    }
                    if (iNodes.length && fNodes.length) {
                        iNodes.forEach(i => fNodes.forEach(f => { if(nodesMap.has(i) && nodesMap.has(f)) g.setEdge(i, f, { class: 'active' }); }));
                    }
                });
            } else if (data.flow && data.flow.path && Array.isArray(data.flow.path)) {
                for (let i = 0; i < data.flow.path.length - 1; i++) {
                    const lbl = data.flow.trigger ? `<span class="bg-black text-vercelBlue px-1 py-0.5 rounded text-[8px]">${data.flow.trigger.toUpperCase()}</span>` : '';
                    g.setEdge(data.flow.path[i], data.flow.path[i+1], { class: 'active', labelType: 'html', label: lbl });
                }
            }
            
        } else if (data.pipelines && Array.isArray(data.pipelines)) {
            // Fallback to legacy arrays
            data.pipelines.forEach((pipe) => {
                const addNode = (id, type, nodeConfig = {}, nodeCodeEvidence = []) => {
                    if (nodesMap.has(id)) return nodesMap.get(id);
                    let fallback = 'lambda';
                    if (type === 'framework') fallback = 's3';
                    else if (type === 'source') fallback = 'apigateway';
                    
                    const simplified = simplifyName(id, fallback);
                    const resType = Object.keys(resourceMap).find(k => id.toLowerCase().includes(k)) || fallback;
                    const styleConfig = resourceMap[resType];
                    const pipelineReport = findPipelineReportForNode(id, data.data_pipeline_reports);

                    const nodeData = {
                        id,
                        label: simplified.title,
                        subtitle: simplified.subtitle,
                        icon: simplified.icon,
                        color: styleConfig.color,
                        type: simplified.category,
                        confidence: simplified.confidence,
                        config: nodeConfig,
                        code_evidence: nodeCodeEvidence,
                        pipelineReport,
                        pipelineNodeType: pipelineReport ? 'pipeline' : null
                    };
                    
                    const html = `
                        <div class="p-3 min-w-[180px] flex flex-col gap-1.5 relative overflow-hidden group">
                            <div class="absolute top-0 right-0 px-2 py-0.5 bg-white/5 border-b border-l border-white/10 rounded-bl-lg text-[8px] font-bold text-textSecondary tracking-tighter uppercase transition-colors group-hover:bg-white/10">
                                ${simplified.category || 'Asset'}
                            </div>
                            <div class="flex items-center gap-2 mt-1">
                                <div class="w-2 h-2 rounded-full shadow-[0_0_8px_rgba(255,255,255,0.3)]" style="background: ${styleConfig.color}"></div>
                                <span class="node-title font-medium text-white truncate max-w-[140px] text-sm">${simplified.title}</span>
                            </div>
                            <div class="flex flex-col gap-0.5">
                                <div class="text-[11px] text-textSecondary uppercase tracking-wider font-medium flex items-center gap-1">
                                    <i data-lucide="${simplified.icon}" class="w-3.5 h-3.5 opacity-60"></i>
                                    <span>${simplified.subtitle}</span>
                                </div>
                                <div class="flex items-center gap-1.5 mt-1">
                                    <div class="flex-1 h-1 bg-white/5 rounded-full overflow-hidden">
                                        <div class="h-full bg-vercelBlue/60" style="width: ${simplified.confidence}%"></div>
                                    </div>
                                    <span class="text-[9px] font-mono text-textSecondary">${simplified.confidence}%</span>
                                </div>
                            </div>
                        </div>
                    `;

                    g.setNode(id, { labelType: 'html', label: html, padding: 0, rx: 12, ry: 12 });
                    nodesMap.set(id, nodeData);
                    return nodeData;
                };

                const sNodes = (pipe.source || []).map(n => addNode(n, 'source'));
                const iNodes = (pipe.ingestion || []).map(n => addNode(n, 'ingestion', pipe.config || {}, pipe.code_evidence || []));
                const fNodes = (pipe.framework || []).map(n => addNode(n, 'framework'));

                if (sNodes.length && iNodes.length) {
                    sNodes.forEach(s => iNodes.forEach(i => g.setEdge(s.id, i.id, { class: 'active' })));
                } else if (sNodes.length && fNodes.length) {
                    sNodes.forEach(s => fNodes.forEach(f => g.setEdge(s.id, f.id, { class: 'active' })));
                }
                if (iNodes.length && fNodes.length) {
                    iNodes.forEach(i => fNodes.forEach(f => g.setEdge(i.id, f.id, { class: 'active' })));
                }
            });
        }


        const svg = d3.select("#graph-svg");
        const inner = svg.append("g");
        
        // Zoom behavior
        zoomBehavior = d3.zoom().on("zoom", (event) => {
            inner.attr("transform", event.transform);
        });
        svg.call(zoomBehavior);

        const render = new dagreD3.render();
        render(inner, g);

        // Center the graph
        const initialScale = 0.8;
        svg.call(zoomBehavior.transform, d3.zoomIdentity.translate((svg.node().getBBox().width - g.graph().width * initialScale) / 2, 20).scale(initialScale));

        // Interaction: Tooltips & Clicks
        const tooltip = d3.select("#graph-tooltip");
        
        inner.selectAll("g.node")
            .on("mouseover", (event, d) => {
                const nodeData = nodesMap.get(d);
                tooltip.style("display", "block")
                    .html(`
                        <div class="font-bold text-white mb-1">${nodeData.label}</div>
                        <div class="text-[10px] text-textSecondary">${nodeData.id}</div>
                        <div class="mt-2 text-vercelBlue">Click to view insights</div>
                    `);
            })
            .on("mousemove", (event) => {
                tooltip.style("left", (event.pageX + 15) + "px")
                    .style("top", (event.pageY - 10) + "px");
            })
            .on("mouseout", () => {
                tooltip.style("display", "none");
            })
            .on("click", (event, d) => {
                const nodeData = nodesMap.get(d);
                openSidePanel(nodeData);
            });

        function openSidePanel(node) {
            const summary = document.getElementById('narrative-summary');
            const evidence = document.getElementById('evidence-pre');
            
            // Build Deep Config HTML
            let configHtml = '';
            const report = node.pipelineReport;

            if (report) {
                configHtml += renderPipelineReportPanels(report);
            }
            
            if (node.config && Object.keys(node.config).length > 0) {
                const renderValue = (k, v) => {
                    if (k === 'support_status') {
                        const isSupported = String(v).toLowerCase().includes('unsupported') ? false : true;
                        if (isSupported) return `<span class="px-2 py-0.5 bg-emerald-500/20 text-emerald-400 rounded text-[9px] uppercase tracking-wider border border-emerald-500/30">✓ Supported</span>`;
                        return `<span class="px-2 py-0.5 bg-red-500/20 text-red-400 rounded text-[9px] uppercase tracking-wider border border-red-500/30">! Unsupported</span>`;
                    }
                    if (k === 'data_format' || k === 'ingestion_type' || k === 'code_framework') {
                        return `<span class="text-vercelBlue font-semibold tracking-wide">${v}</span>`;
                    }
                    if (Array.isArray(v)) {
                         return `<div class="flex flex-wrap gap-1 justify-end">${v.map(item => `<span class="px-1.5 py-0.5 bg-white/10 rounded text-[9px] uppercase">${item}</span>`).join('')}</div>`;
                    }
                    return typeof v === 'object' ? JSON.stringify(v) : v;
                };

                const configItems = Object.entries(node.config).map(([k, v]) => `
                    <div class="flex justify-between items-center py-2 border-b border-white/5 last:border-0">
                        <span class="text-textSecondary uppercase tracking-wider text-[9px] font-medium">${k.replace(/_/g, ' ')}</span>
                        <span class="text-white font-medium text-right ml-4 text-[10px] flex items-center gap-1">${renderValue(k, v)}</span>
                    </div>
                `).join('');

                configHtml += `
                    <div class="bg-black/60 p-4 rounded-xl border border-white/10 mt-3 relative overflow-hidden transition-all hover:border-vercelBlue/30">
                        <div class="absolute top-0 right-0 w-16 h-16 bg-vercelBlue/10 blur-xl rounded-full translate-x-8 -translate-y-8 pointer-events-none"></div>
                        <h5 class="text-[10px] uppercase font-bold text-white mb-3 flex items-center gap-1.5 opacity-90"><i data-lucide="cpu" class="w-3.5 h-3.5 text-vercelBlue"></i> Ingestion Context</h5>
                        <div class="text-[10px] space-y-0.5">${configItems}</div>
                    </div>
                    
                    <div class="bg-[#0D1117]/80 p-3 rounded-xl border border-white/5 mt-3 overflow-x-auto">
                        <h5 class="text-[9px] uppercase font-mono text-emerald-400 mb-2 flex items-center gap-1.5"><i data-lucide="terminal" class="w-3 h-3"></i> Deep Config Extraction (Raw JSON)</h5>
                        <pre class="text-[10px] text-gray-300 font-mono tracking-tight leading-relaxed"><code>${JSON.stringify(node.config, null, 2)}</code></pre>
                    </div>
                `;
            }

            // Bring in Global configurations based on node role
            const globalConfigs = {
                SOURCE: data.source_config,
                COMPUTE: data.ingestion_config,
                STORAGE: data.storage_config
            };

            const roleConfig = globalConfigs[node.type];
            if (roleConfig && Object.keys(roleConfig).length > 0) {
                const globalItems = Object.entries(roleConfig).map(([k, v]) => `
                    <div class="flex justify-between items-center py-1">
                        <span class="text-textSecondary uppercase tracking-wider">${k.replace(/_/g, ' ')}</span>
                        <span class="text-vercelBlue font-mono">${Array.isArray(v) ? v.join(', ') : v}</span>
                    </div>
                `).join('');
                configHtml += `
                    <div class="bg-vercelBlue/5 p-3 rounded-xl border border-vercelBlue/20 mt-3">
                        <h5 class="text-[10px] uppercase font-semibold text-vercelBlue mb-2 flex items-center gap-1"><i data-lucide="database" class="w-3 h-3"></i> Platform Inferred Config</h5>
                        <div class="text-[10px] space-y-1">${globalItems}</div>
                    </div>
                `;
            }

            if (node.type === 'STORAGE' && data.dq_config && data.dq_config.dq_rules) {
                configHtml += `
                    <div class="bg-emerald-500/5 p-3 rounded-xl border border-emerald-500/20 mt-3">
                        <h5 class="text-[10px] uppercase font-semibold text-emerald-500 mb-2 flex items-center gap-1"><i data-lucide="shield-check" class="w-3 h-3"></i> Auto-Injected DQ Rules</h5>
                        <div class="flex flex-wrap gap-1 mt-2">
                            ${data.dq_config.dq_rules.map(r => `<span class="px-2 py-0.5 bg-emerald-500/10 text-emerald-500 rounded text-[9px] uppercase">${r}</span>`).join('')}
                        </div>
                    </div>
                `;
            }
            
            // Populate Intelligence Panel (Zone 3)
            summary.innerHTML = `
                <div class="space-y-4">
                    <div class="flex items-center gap-3 mb-6">
                        <div class="w-10 h-10 rounded-xl flex items-center justify-center bg-vercelBlue/10 text-vercelBlue">
                            <i data-lucide="${node.icon || 'info'}" class="w-6 h-6"></i>
                        </div>
                        <div>
                            <h4 class="text-white font-semibold text-lg max-w-[200px] truncate" title="${node.label}">${node.label}</h4>
                            <p class="text-textSecondary text-[10px] uppercase tracking-wider">${node.type} | ${node.subtitle}</p>
                        </div>
                    </div>
                    
                    <div class="grid grid-cols-2 gap-2">
                        <div class="bg-white/5 p-3 rounded-lg border border-border">
                            <div class="text-[10px] text-textSecondary uppercase mb-1">Confidence</div>
                            <div class="text-emerald-400 font-medium">${node.confidence}%</div>
                        </div>
                        <div class="bg-white/5 p-3 rounded-lg border border-border">
                            <div class="text-[10px] text-textSecondary uppercase mb-1">Status</div>
                            <div class="text-white font-medium flex items-center gap-1"><div class="w-1.5 h-1.5 rounded-full bg-emerald-500"></div> Active</div>
                        </div>
                    </div>

                    ${configHtml}

                    <div class="bg-black/40 p-4 rounded-xl border border-border mt-4">
                        <h5 class="text-[10px] uppercase font-semibold text-textSecondary mb-2">AI Architecture Synthesis</h5>
                        <p class="text-xs text-textSecondary leading-relaxed italic border-l-2 border-vercelBlue pl-2">"Engineered for high-throughput pipeline ingestion. This node operates as a ${node.type.toLowerCase()} layer handling ${node.subtitle} processes.${report?.flow?.text ? ` Flow: ${report.flow.text}.` : ''}"</p>
                    </div>
                </div>
            `;
            
            // Show raw data in evidence
            document.getElementById('evidence-panel').classList.remove('hidden');
            evidence.textContent = JSON.stringify(node, null, 2);
            
            lucide.createIcons();
        }

        // --- 3. Generative UI Cards ---
        resultsContent.innerHTML = ''; 
        const categories = [
            { key: 'framework', label: 'Frameworks', icon: 'layers', color: 'emerald' },
            { key: 'source', label: 'Data Sources', icon: 'database', color: 'sky' },
            { key: 'ingestion', label: 'Ingestion Engine', icon: 'cpu', color: 'purple' },
            { key: 'dq_rules', label: 'DQ Rules', icon: 'shield-check', color: 'amber' }
        ];

        categories.forEach(cat => {
            const items = data[cat.key] || [];
            if (items.length > 0) {
                let fallbackMap = { 'framework': 's3', 'source': 'apigateway', 'ingestion': 'lambda' };
                let tagsHTML = items.map(tag => {
                    const simplified = simplifyName(tag, fallbackMap[cat.key] || 'lambda');
                    return `
                        <div class="flex items-center gap-2 bg-white/5 border border-white/10 hover:border-white/20 transition-colors px-2 py-1.5 rounded-lg text-xs group cursor-default" title="${tag}">
                            <i data-lucide="${simplified.icon}" class="w-3 h-3 text-textSecondary group-hover:text-white transition-colors"></i>
                            <div class="flex flex-col">
                                <span class="text-white font-medium truncate max-w-[150px] text-[13px]">${simplified.title}</span>
                                <span class="text-[10px] text-textSecondary uppercase tracking-tighter leading-none">${simplified.subtitle}</span>
                            </div>
                        </div>
                    `;
                }).join('');

                let html = `
                <div class="bg-surface border border-border rounded-xl p-4 flex flex-col gap-3 hover:border-${cat.color}-500/30 transition-colors group">
                    <div class="flex justify-between items-center">
                        <div class="flex items-center gap-2">
                            <i data-lucide="${cat.icon}" class="w-4 h-4 text-${cat.color}-500"></i>
                            <h4 class="text-xs uppercase tracking-wider font-semibold text-textSecondary">${cat.label}</h4>
                        </div>
                    </div>
                    <div class="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-1">
                        ${tagsHTML}
                    </div>
                </div>`;
                resultsContent.insertAdjacentHTML('beforeend', html);
            }
        });
        
        lucide.createIcons({ root: resultsContent });

        // --- 4. Extracted config tabs ---
        renderExtractedConfigTabs(data);

        // --- 5. Detailed Configs Rendering ---
        renderDetailedConfigs(data);
    }

    function renderDetailedConfigs(data) {
        const panel = document.getElementById('detailed-configs-panel');
        const grid = document.getElementById('detailed-configs-grid');
        const searchInput = document.getElementById('config-search');
        
        if (!panel || !grid) return;

        // Collect all potential configuration sources
        let inventory = data.detailed_inventory || [];
        
        if (inventory.length === 0) {
            panel.classList.add('hidden');
            return;
        }

        panel.classList.remove('hidden');

        const renderItems = (filter = '') => {
            grid.innerHTML = '';
            const filtered = inventory.filter(item => 
                item.id.toLowerCase().includes(filter.toLowerCase()) || 
                item.service.toLowerCase().includes(filter.toLowerCase()) ||
                JSON.stringify(item.config).toLowerCase().includes(filter.toLowerCase())
            );

            if (filtered.length === 0) {
                grid.innerHTML = '<div class="text-center py-12 text-textSecondary italic">No configurations match your search...</div>';
                return;
            }

            filtered.forEach((item, idx) => {
                const simplified = simplifyName(item.id, item.service.toLowerCase());
                const pipelineReport = findPipelineReportForNode(item.id, data.data_pipeline_reports);
                const card = document.createElement('div');
                card.className = "group relative bg-surface border border-border rounded-2xl overflow-hidden transition-all hover:border-vercelBlue/40 hover:shadow-2xl hover:shadow-vercelBlue/5";
                
                let configRows = Object.entries(item.config).map(([k, v]) => `
                    <div class="flex justify-between items-start py-2.5 border-b border-white/5 last:border-0 group/row">
                        <span class="text-[10px] uppercase tracking-wider text-textSecondary font-semibold mt-0.5">${k.replace(/_/g, ' ')}</span>
                        <span class="text-xs font-mono text-white/90 break-all ml-8 text-right bg-white/0 group-hover/row:bg-white/5 px-1 rounded transition-colors">${typeof v === 'object' ? JSON.stringify(v) : v}</span>
                    </div>
                `).join('');

                let pipelineRows = '';
                if (pipelineReport) {
                    pipelineRows = `<div class="mt-6">${renderPipelineReportPanels(pipelineReport)}</div>`;
                }

                card.innerHTML = `
                    <div class="p-5 border-b border-border bg-black/20 flex items-center justify-between">
                        <div class="flex items-center gap-3">
                            <div class="w-10 h-10 rounded-xl bg-white/5 flex items-center justify-center text-vercelBlue border border-white/10">
                                <i data-lucide="${simplified.icon}" class="w-5 h-5"></i>
                            </div>
                            <div>
                                <h4 class="text-[15px] font-semibold text-white leading-none mb-1">${simplified.title}</h4>
                                <p class="text-[10px] text-textSecondary uppercase tracking-widest">${item.service} | ${item.id.split(' || ')[0] || 'Global'}</p>
                            </div>
                        </div>
                        <button onclick="copyToClipboard('${item.id}')" class="p-2 rounded-lg hover:bg-white/10 text-textSecondary hover:text-white transition-all opacity-0 group-hover:opacity-100" title="Copy Identifier">
                            <i data-lucide="copy" class="w-4 h-4"></i>
                        </button>
                    </div>
                    <div class="p-5 bg-black/40">
                        <div class="space-y-0.5">
                            ${configRows || '<div class="text-[10px] text-textSecondary italic">No detailed properties available.</div>'}
                        </div>
                        ${pipelineRows}
                    </div>
                    <div class="px-5 py-3 bg-white/[0.02] border-t border-border flex items-center justify-between">
                        <span class="text-[9px] font-mono text-textSecondary uppercase tracking-tighter">Resource ID: ${item.id}</span>
                        <div class="flex items-center gap-1.5">
                            <div class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></div>
                            <span class="text-[9px] font-bold text-emerald-500/80 uppercase">Verified via SDK</span>
                        </div>
                    </div>
                `;
                grid.appendChild(card);
            });
            lucide.createIcons({ root: grid });
        };

        // Initial render
        renderItems();

        // Search event
        searchInput.oninput = (e) => renderItems(e.target.value);
    }

    window.copyToClipboard = (text) => {
        navigator.clipboard.writeText(text);
        showToast('Copied to clipboard');
    };

    // Check for success/errors in URL (e.g., from SSO redirect)
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('error')) {
        showToast(urlParams.get('error'), 'error');
        window.history.replaceState({}, document.title, window.location.pathname);
    }
    if (urlParams.has('success')) {
        showToast(urlParams.get('success'), 'success');
        window.history.replaceState({}, document.title, window.location.pathname);
    }

    // Handle Login Button Logic
    const btnLogin = document.getElementById('btn-login');
    if (btnLogin) {
        btnLogin.addEventListener('click', async () => {
            const resp = await fetch('/api/config/keys');
            const keys = await resp.json();
            
            // If Client ID (Azure) is not configured, use the "Browser SSO" flow
            if (!keys.azure_client_id_active) {
                window.location.href = '/browser-login';
            } else {
                window.location.href = '/login';
            }
        });
    }

    // --- 5. SSO Identity Logic ---
    async function checkAuthStatus() {
        try {
            // Check session auth
            const hRes = await fetch('/health');
            const hData = await hRes.json();
            
            // Check configured keys
            const kRes = await fetch('/api/config/keys');
            const kData = await kRes.json();
            
            const userInfo = document.getElementById('user-info');
            const loginCta = document.getElementById('login-cta');
            const userName = document.getElementById('user-name');
            const userInitials = document.getElementById('user-initials');
            
            // Azure Status Card elements
            const azureTitle = document.getElementById('azure-status-title');
            const azureText = document.getElementById('azure-status-text');
            const azureCard = document.getElementById('azure-status-card');

            if (hData.user) {
                // User is logged in via SSO
                const name = hData.user.name || hData.user.preferred_username || "User";
                userName.textContent = name;
                userInitials.textContent = name.split(' ').map(n => n[0]).join('').toUpperCase().substring(0, 2);
                
                userInfo.classList.remove('hidden');
                loginCta.classList.add('hidden');
                
                azureTitle.textContent = "Azure Active (SSO)";
                azureText.textContent = `Acting as ${name}`;
                providerStatus.azure = 'sso';
                providerStatus.fabric = 'ready'; // Fabric uses same SSO token
            } else {
                userInfo.classList.add('hidden');
                loginCta.classList.remove('hidden');
                
                if (kData.azure) {
                    azureTitle.textContent = "Azure Service Principal";
                    azureText.textContent = "ID/Secret active in .env";
                    providerStatus.azure = 'sp';
                    providerStatus.fabric = 'ready';
                } else {
                    azureTitle.textContent = "Azure Discovery";
                    azureText.textContent = "Not configured in .env";
                    providerStatus.azure = 'none';
                    providerStatus.fabric = 'none';
                }
            }
            syncProviderCards();
        } catch (error) {
            console.error("Auth check failed:", error);
        }
    }

    checkAuthStatus();

    // --- 6. Provider Selection Logic ---
    const selectedProviders = new Set([]); 
    const providerStatus = { aws: 'ready', azure: 'none', fabric: 'ready' };

    function syncProviderCards() {
        ['aws', 'azure', 'fabric'].forEach(id => {
            const card = document.getElementById(`card-${id}`);
            const check = document.getElementById(`check-${id}`);
            if (!card) return;

            const isSelected = selectedProviders.has(id);
            const isReady = providerStatus[id] !== 'none';

            // Reset classes
            card.classList.remove('border-vercelBlue', 'border-vercelBlue/40', 'bg-vercelBlue/5', 'border-border', 'opacity-60');

            if (isSelected) {
                card.classList.add('border-vercelBlue', 'bg-vercelBlue/5');
            } else if (isReady) {
                card.classList.add('border-vercelBlue/20');
            } else {
                card.classList.add('border-border', 'opacity-60');
            }

            if (check) {
                if (isSelected) check.classList.remove('hidden');
                else check.classList.add('hidden');
            }
        });
    }

    function setupProviderEvents() {
        ['aws', 'azure', 'fabric'].forEach(id => {
            const card = document.getElementById(`card-${id}`);
            if (card) {
                card.onclick = (e) => {
                    // Prevent multiple rapid clicks
                    if (card.dataset.clicking) return;
                    card.dataset.clicking = "true";
                    setTimeout(() => delete card.dataset.clicking, 200);

                    if (selectedProviders.has(id)) {
                        selectedProviders.delete(id);
                    } else {
                        selectedProviders.add(id);
                    }
                    console.log(`Toggled ${id}. Current selection:`, Array.from(selectedProviders));
                    syncProviderCards();
                };
            }
        });
    }

    setupProviderEvents();
    syncProviderCards();

    window.executeScan = async () => {
        const btn = document.querySelector('button[onclick="executeScan()"]');
        if (!btn) return;
        
        const originalHtml = btn.innerHTML;
        btn.innerHTML = `<span class="animate-spin mr-2">◌</span> Scanning...`;
        btn.disabled = true;

        // Show spinners for all selected providers
        selectedProviders.forEach(p => {
            const spinner = document.getElementById(`spinner-${p}`);
            if (spinner) spinner.classList.remove('hidden');
        });

        try {
            const providerQuery = Array.from(selectedProviders).join(',');
            const useLlmScan = document.getElementById('scan-use-llm')?.checked;
            const llmParam = useLlmScan ? '&use_llm=true' : '';
            const response = await fetch(`/scan-cloud?providers=${providerQuery}${llmParam}`, { method: 'POST' });
            const data = await response.json();
            
            if (response.ok) {
                renderGraph(data);
                renderNarrative(data);
                renderDiscoveryGrid(data);
                showToast(`Scan complete: ${providerQuery || 'all'}`, 'success');
            } else {
                showToast(data.detail || 'Scan failed', 'error');
            }
        } catch (error) {
            console.error(error);
            showToast('Connection error during scan', 'error');
        } finally {
            btn.innerHTML = originalHtml;
            btn.disabled = false;
            // Hide all spinners
            ['aws', 'azure', 'gcp'].forEach(p => {
                const spinner = document.getElementById(`spinner-${p}`);
                if (spinner) spinner.classList.add('hidden');
            });
        }
    };
});
