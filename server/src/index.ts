import { WebSocketServer, WebSocket } from 'ws';
import { Player, Question, Game, User, WSMessage, RegData, CreateGameData, JoinGameData, StartGameData, AnswerData } from './types';

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 3000;

const users = new Map<string, User>();
const games = new Map<string, Game>();
const codes = new Map<string, Game>();

const wss = new WebSocketServer({ port: PORT });

function generateCode(): string {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

function send(ws: WebSocket, message: WSMessage) {
  ws.send(JSON.stringify(message));
}

function broadcast(game: Game, message: WSMessage, exclude?: WebSocket) {
  game.players.forEach(player => {
    if (player.ws && player.ws !== exclude) {
      send(player.ws, message);
    }
  });
  const host = users.get(game.hostId);
  if (host && host.ws && host.ws !== exclude) {
    send(host.ws, message);
  }
}

wss.on('connection', (ws) => {
  ws.on('message', (data) => {
    try {
      const message: WSMessage = JSON.parse(data.toString());
      handleMessage(ws, message);
    } catch (e) {
      console.error('Invalid message', e);
    }
  });

  ws.on('close', () => {
    handleDisconnect(ws);
  });
});

function handleMessage(ws: WebSocket, message: WSMessage) {
  switch (message.type) {
    case 'reg':
      handleReg(ws, message.data as RegData);
      break;
    case 'create_game':
      handleCreateGame(ws, message.data as CreateGameData);
      break;
    case 'join_game':
      handleJoinGame(ws, message.data as JoinGameData);
      break;
    case 'start_game':
      handleStartGame(ws, message.data as StartGameData);
      break;
    case 'answer':
      handleAnswer(ws, message.data as AnswerData);
      break;
    default:
      send(ws, { type: 'error', data: { errorText: 'Unknown command' }, id: 0 });
  }
}

function handleReg(ws: WebSocket, data: RegData) {
  let user = users.get(data.name);
  if (!user) {
    user = {
      name: data.name,
      password: data.password,
      index: Math.random().toString(36).substring(2),
      ws
    };
    users.set(data.name, user);
  } else {
    if (user.password !== data.password) {
      send(ws, { type: 'reg', data: { name: data.name, index: '', error: true, errorText: 'Wrong password' }, id: 0 });
      return;
    }
    user.ws = ws;
  }
  send(ws, { type: 'reg', data: { name: data.name, index: user.index, error: false, errorText: '' }, id: 0 });
}

function handleCreateGame(ws: WebSocket, data: CreateGameData) {
  let host: User | undefined;
  for (const u of users.values()) {
    if (u.ws === ws) {
      host = u;
      break;
    }
  }

  if (!host) {
    send(ws, { type: 'error', data: { errorText: 'Not registered' }, id: 0 });
    return;
  }

  if (!data.questions || data.questions.length === 0) {
    send(ws, { type: 'error', data: { errorText: 'No questions provided' }, id: 0 });
    return;
  }

  for (const q of data.questions) {
    if (!q.text || q.options.length !== 4 || q.correctIndex < 0 || q.correctIndex > 3 || q.timeLimitSec <= 0) {
      send(ws, { type: 'error', data: { errorText: 'Invalid question format' }, id: 0 });
      return;
    }
  }

  const gameId = Math.random().toString(36).substring(2);
  let code: string;
  do {
    code = generateCode();
  } while (codes.has(code)); 

  const game: Game = {
    id: gameId,
    code,
    hostId: host.index,
    questions: data.questions,
    players: [], 
    currentQuestion: -1,
    status: 'waiting',
    playerAnswers: new Map()
  };

  games.set(gameId, game);
  codes.set(code, game);

  send(ws, { 
    type: 'update_players', 
    data: [], 
    id: 0 
  });

  send(ws, { type: 'game_created', data: { gameId, code }, id: 0 });
}

function handleJoinGame(ws: WebSocket, data: JoinGameData) {
  const game = codes.get(data.code);
  if (!game) {
    send(ws, { type: 'error', data: { errorText: 'Game not found' }, id: 0 });
    return;
  }
  if (game.status !== 'waiting') {
    send(ws, { type: 'error', data: { errorText: 'Game already started' }, id: 0 });
    return;
  }

  let user: User | undefined;
  for (const u of users.values()) {
    if (u.ws === ws) {
      user = u;
      break;
    }
  }
  if (!user) {
    send(ws, { type: 'error', data: { errorText: 'Not registered' }, id: 0 });
    return;
  }

  if (game.players.some(p => p.index === user!.index)) {
    send(ws, { type: 'error', data: { errorText: 'Already in game' }, id: 0 });
    return;
  }

  const player: Player = {
    name: user.name,
    index: user.index,
    score: 0,
    ws
  };

  game.players.push(player);

  send(ws, { type: 'game_joined', data: { gameId: game.id }, id: 0 });

  broadcast(game, { type: 'player_joined', data: { playerName: player.name, playerCount: game.players.length }, id: 0 });

  broadcast(game, { type: 'update_players', data: game.players.map(p => ({ name: p.name, index: p.index, score: p.score })), id: 0 });
}

function handleStartGame(ws: WebSocket, data: StartGameData) {
  const game = games.get(data.gameId);
  if (!game) {
    send(ws, { type: 'error', data: { errorText: 'Game not found' }, id: 0 });
    return;
  }

  if (game.hostId !== getUserByWs(ws)?.index) {
    send(ws, { type: 'error', data: { errorText: 'Not host' }, id: 0 });
    return;
  }

  if (game.status !== 'waiting') {
    send(ws, { type: 'error', data: { errorText: 'Game not waiting' }, id: 0 });
    return;
  }

  game.status = 'in_progress';
  game.currentQuestion = 0;
  sendQuestion(game);
}

function sendQuestion(game: Game) {
  const q = game.questions[game.currentQuestion];
  game.questionStartTime = Date.now();
  game.playerAnswers.clear();

  broadcast(game, {
    type: 'question',
    data: {
      questionNumber: game.currentQuestion + 1,
      totalQuestions: game.questions.length,
      text: q.text,
      options: q.options,
      timeLimitSec: q.timeLimitSec
    },
    id: 0
  });

  game.questionTimer = setTimeout(() => {
    endQuestion(game);
  }, q.timeLimitSec * 1000);
}

function endQuestion(game: Game) {
  if (game.questionTimer) {
    clearTimeout(game.questionTimer);
    game.questionTimer = undefined;
  }

  const q = game.questions[game.currentQuestion];
  const correctIndex = q.correctIndex;
  const timeLimit = q.timeLimitSec;
  const startTime = game.questionStartTime!;

  const playerResults = game.players.map(player => {
    const answer = game.playerAnswers.get(player.index);
    let answered = false;
    let correct = false;
    let pointsEarned = 0;
    if (answer) {
      answered = true;
      correct = answer.answerIndex === correctIndex;
      if (correct) {
        const timeTaken = (answer.timestamp - startTime) / 1000;
        const timeRemaining = Math.max(0, timeLimit - timeTaken);
        pointsEarned = Math.round(1000 * (timeRemaining / timeLimit));
        player.score += pointsEarned;
      }
    }
    return {
      name: player.name,
      answered,
      correct,
      pointsEarned,
      totalScore: player.score
    };
  });

  broadcast(game, {
    type: 'question_result',
    data: {
      questionIndex: game.currentQuestion,
      correctIndex,
      playerResults
    },
    id: 0
  });

  game.currentQuestion++;
  if (game.currentQuestion < game.questions.length) {
    setTimeout(() => sendQuestion(game), 3000); 
  } else {
    game.status = 'finished';
    const scoreboard = game.players
      .map(p => ({ name: p.name, score: p.score }))
      .sort((a, b) => b.score - a.score)
      .map((p, i) => ({ ...p, rank: i + 1 }));
    broadcast(game, {
      type: 'game_finished',
      data: { scoreboard },
      id: 0
    });
  }
}

function handleAnswer(ws: WebSocket, data: AnswerData) {
  const game = games.get(data.gameId);
  if (!game || game.status !== 'in_progress' || game.currentQuestion !== data.questionIndex) {
    send(ws, { type: 'error', data: { errorText: 'Invalid answer' }, id: 0 });
    return;
  }

  const player = game.players.find(p => p.ws === ws);
  if (!player) {
    send(ws, { type: 'error', data: { errorText: 'Not in game' }, id: 0 });
    return;
  }

  if (game.playerAnswers.has(player.index)) {
    send(ws, { type: 'error', data: { errorText: 'Already answered' }, id: 0 });
    return;
  }

  game.playerAnswers.set(player.index, { answerIndex: data.answerIndex, timestamp: Date.now() });

  send(ws, { type: 'answer_accepted', data: { questionIndex: data.questionIndex }, id: 0 });

  if (game.playerAnswers.size === game.players.length) {
    endQuestion(game);
  }
}

function handleDisconnect(ws: WebSocket) {
  let user: User | undefined;
  for (const u of users.values()) {
    if (u.ws === ws) {
      user = u;
      u.ws = undefined;
      break;
    }
  }

  for (const game of games.values()) {
    const playerIndex = game.players.findIndex(p => p.ws === ws);
    if (playerIndex !== -1) {
      game.players.splice(playerIndex, 1);
      broadcast(game, { type: 'update_players', data: game.players.map(p => ({ name: p.name, index: p.index, score: p.score })), id: 0 });
    }
  }
}

function getUserByWs(ws: WebSocket): User | undefined {
  for (const u of users.values()) {
    if (u.ws === ws) return u;
  }
}

console.log(`WebSocket server started on ws://localhost:${PORT}`);