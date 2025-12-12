const express = require('express');
const cors = require('cors');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Stockage des sessions de génération
const activeSessions = new Map();

// Route pour démarrer la génération
app.post('/api/generate', (req, res) => {
    const sessionId = Date.now().toString();
    
    // Chemin vers le script Python
    const pythonScriptPath = path.join(__dirname, 'Facturesauto.py');
    
    // Vérifier si le script existe
    if (!fs.existsSync(pythonScriptPath)) {
        console.error(`❌ Script Python introuvable: ${pythonScriptPath}`);
        return res.status(404).json({ 
            success: false, 
            error: 'Script Python introuvable' 
        });
    }
    
    console.log(`🚀 Lancement de la génération pour la session: ${sessionId}`);
    
    // Initialiser la session
    const session = {
        id: sessionId,
        status: 'starting',
        progress: 0,
        message: 'Initialisation du système...',
        urls: [],
        clientsProcessed: 0,
        totalClients: 0,
        currentClient: '',
        error: null,
        startTime: new Date(),
        logs: []
    };
    
    activeSessions.set(sessionId, session);
    
    // Lancer le script Python avec gestion correcte des chemins
    const pythonProcess = spawn('python3', [pythonScriptPath], {
        cwd: path.dirname(pythonScriptPath),
        shell: true
    });
    
    // Fonction pour ajouter un log
    const addLog = (message, type = 'info') => {
        const logEntry = {
            timestamp: new Date().toISOString(),
            message: message,
            type: type
        };
        session.logs.push(logEntry);
        console.log(`[${type.toUpperCase()}] ${message}`);
        
        // Garder seulement les 100 derniers logs
        if (session.logs.length > 100) {
            session.logs = session.logs.slice(-100);
        }
    };
    
    // Capturer la sortie stdout
    pythonProcess.stdout.on('data', (data) => {
        const output = data.toString().trim();
        if (!output) return;
        
        addLog(output, 'info');
        
        // Détecter les messages de progression
        if (output.includes('PROGRESS:')) {
            try {
                const match = output.match(/PROGRESS:({.*})/);
                if (match) {
                    const progressData = JSON.parse(match[1]);
                    
                    // Mettre à jour la session
                    session.status = progressData.status || session.status;
                    session.message = progressData.message || session.message;
                    session.clientsProcessed = progressData.step || session.clientsProcessed;
                    session.totalClients = progressData.total_steps || session.totalClients;
                    
                    // Assurer que la progression est monotone (jamais en arrière)
                    const newProgress = progressData.progress !== undefined ? progressData.progress : session.progress;
                    if (newProgress >= session.progress) {
                        session.progress = Math.min(newProgress, 99); // Max 99% avant le SUMMARY
                    }
                    // Si newProgress < session.progress, on garde la valeur actuelle (progression monotone)
                    
                    if (progressData.urls && Array.isArray(progressData.urls)) {
                        session.urls = progressData.urls;
                    }
                    
                    if (progressData.error) {
                        session.error = progressData.error;
                        session.status = 'error';
                    }
                    
                    // Extraire le nom du client du message
                    if (session.message.includes('pour:')) {
                        const clientMatch = session.message.match(/pour:\s*(.+)/);
                        if (clientMatch) {
                            session.currentClient = clientMatch[1].trim();
                        }
                    } else if (session.message.includes('Génération pour')) {
                        const clientMatch = session.message.match(/Génération pour\s*(.+)/);
                        if (clientMatch) {
                            session.currentClient = clientMatch[1].trim();
                        }
                    }
                    
                    console.log(`📊 Progression: ${session.progress}% - ${session.message}`);
                }
            } catch (e) {
                addLog(`Erreur parsing JSON: ${e.message}`, 'error');
            }
        }
        
        // Détecter le résumé final
        if (output.includes('SUMMARY:')) {
            try {
                const match = output.match(/SUMMARY:({.*})/);
                if (match) {
                    const summaryData = JSON.parse(match[1]);
                    session.summary = summaryData;
                    session.status = 'completed';
                    session.progress = 100;
                    session.clientsProcessed = summaryData.total_clients || 0;
                    session.totalClients = summaryData.total_clients || 0;
                    session.endTime = new Date();
                    session.message = `✅ Génération terminée! ${summaryData.factures_generees || 0} factures créées en ${summaryData.duree || '0s'}`;
                    
                    addLog('✅ Génération terminée avec succès', 'success');
                    console.log('📊 Résumé:', summaryData);
                }
            } catch (e) {
                addLog(`Erreur parsing summary: ${e.message}`, 'error');
            }
        }
        
        // Détection intelligente de la progression à partir des logs
        if (output.includes('👤 Traitement client')) {
            const match = output.match(/Traitement client\s+(\d+)\/(\d+)/);
            if (match) {
                session.clientsProcessed = parseInt(match[1]);
                session.totalClients = parseInt(match[2]);
                
                if (session.totalClients > 0) {
                    const calc = Math.min(95, Math.round((session.clientsProcessed / session.totalClients) * 100));
                    if (calc >= session.progress) {
                        session.progress = calc;
                    }
                }
                
                session.status = 'generating';
            }
        }
        
        if (output.includes('📄 Génération facture pour :')) {
            const match = output.match(/Génération facture pour :\s*(.+)/);
            if (match) {
                session.currentClient = match[1].trim();
                session.message = `Génération pour: ${session.currentClient}`;
            }
        }
        
        if (output.includes('✅ PDF généré temporairement')) {
            session.status = 'converting';
            session.message = `Conversion PDF pour: ${session.currentClient}`;
        }
        
        if (output.includes('☁️  Upload du PDF vers Cloudinary')) {
            session.status = 'uploading';
            session.message = `Upload vers Cloudinary pour: ${session.currentClient}`;
        }
        
        if (output.includes('✅ Fichier téléversé avec succès')) {
            // Compter comme une facture générée
            if (session.totalClients > 0) {
                const calcUp = Math.min(95, Math.round((session.urls.length / session.totalClients) * 100));
                if (calcUp >= session.progress) {
                    session.progress = calcUp;
                }
            }
            session.message = `✅ ${session.currentClient} - Facture uploadée`;
        }
        
        // Détecter les erreurs
        if (output.includes('❌') || (output.includes('Erreur') && !output.includes('✅'))) {
            session.error = output;
            session.status = 'error';
        }
    });
    
    // Capturer les erreurs stderr
    pythonProcess.stderr.on('data', (data) => {
        const error = data.toString().trim();
        if (!error) return;
        
        addLog(`ERREUR: ${error}`, 'error');
        session.error = error;
        session.status = 'error';
    });
    
    // Gérer la fin du processus
    pythonProcess.on('close', (code) => {
        console.log(`🔚 Processus Python terminé avec code: ${code}`);
        
        if (code !== 0 && session.status !== 'completed') {
            session.status = 'error';
            session.message = 'Erreur lors de l\'exécution du script Python';
            if (!session.error) {
                session.error = `Le script s'est terminé avec le code d'erreur: ${code}`;
            }
        }
        
        if (session.status === 'completed') {
            session.endTime = new Date();
            const duration = Math.round((session.endTime - session.startTime) / 1000);
            session.message = `✅ Génération terminée en ${duration}s - ${session.urls.length} factures créées`;
        }
        
        // Garder la session pendant 10 minutes après la fin
        setTimeout(() => {
            if (activeSessions.has(sessionId)) {
                activeSessions.delete(sessionId);
                console.log(`🗑️ Session ${sessionId} supprimée`);
            }
        }, 10 * 60 * 1000);
    });
    
    // Gérer les erreurs du processus
    pythonProcess.on('error', (err) => {
        console.error('❌ Erreur du processus Python:', err);
        session.error = `Impossible de démarrer le script Python: ${err.message}`;
        session.status = 'error';
    });
    
    res.json({
        success: true,
        sessionId: sessionId,
        message: 'Génération démarrée'
    });
});

// Route pour obtenir la progression
app.get('/api/progress/:sessionId', (req, res) => {
    const sessionId = req.params.sessionId;
    const session = activeSessions.get(sessionId);
    
    if (!session) {
        return res.status(404).json({
            success: false,
            error: 'Session non trouvée'
        });
    }
    
    res.json({
        success: true,
        session: {
            id: session.id,
            status: session.status,
            progress: session.progress,
            message: session.message,
            clientsProcessed: session.clientsProcessed,
            totalClients: session.totalClients,
            currentClient: session.currentClient,
            urls: session.urls,
            error: session.error,
            startTime: session.startTime,
            endTime: session.endTime,
            summary: session.summary
        }
    });
});

// Route pour lister toutes les sessions actives
app.get('/api/sessions', (req, res) => {
    const sessions = Array.from(activeSessions.entries()).map(([id, session]) => ({
        id: id,
        status: session.status,
        progress: session.progress,
        message: session.message,
        clientsProcessed: session.clientsProcessed,
        totalClients: session.totalClients,
        startTime: session.startTime,
        duration: session.endTime ? 
            Math.round((session.endTime - session.startTime) / 1000) : 
            Math.round((new Date() - session.startTime) / 1000)
    }));
    
    res.json({
        success: true,
        sessions: sessions
    });
});

// Route pour vérifier la santé du serveur
app.get('/api/health', (req, res) => {
    res.json({
        success: true,
        status: 'running',
        timestamp: new Date().toISOString(),
        activeSessions: activeSessions.size
    });
});

// Route pour obtenir les logs d'une session
app.get('/api/logs/:sessionId', (req, res) => {
    const sessionId = req.params.sessionId;
    const session = activeSessions.get(sessionId);
    
    if (!session) {
        return res.status(404).json({
            success: false,
            error: 'Session non trouvée'
        });
    }
    
    res.json({
        success: true,
        logs: session.logs.slice(-50)
    });
});

// Servir le frontend
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Route pour servir les fichiers statiques
app.get('/:filename', (req, res) => {
    const filename = req.params.filename;
    const filePath = path.join(__dirname, 'public', filename);
    
    if (fs.existsSync(filePath)) {
        res.sendFile(filePath);
    } else {
        res.status(404).send('Fichier non trouvé');
    }
});

app.listen(PORT, () => {
    console.log(`✅ Serveur backend démarré sur http://localhost:${PORT}`);
   });