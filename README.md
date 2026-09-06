# Discord ScreenStream

Transmissão de tela com áudio e observadores via WebRTC. Execute `npm ci` e `npm start`; abra `http://localhost:3000`. Acesso remoto à captura e ao microfone requer HTTPS.

## Áudio

1. **Já estou em uma call** começa marcado. Nesse modo, a voz continua no Discord ou no aplicativo da call, e o sistema não solicita um segundo microfone.
2. Clique em **Transmitir Tela**, escolha a **aba do conteúdo** e marque **Compartilhar áudio**. Mantenha o Discord/call em outra aba ou aplicativo: se compartilhar a própria aba da call, suas vozes também estarão nesse áudio.
3. Em modo de call, compartilhar uma janela ou tela inteira transmite **somente vídeo**. O áudio geral do computador é bloqueado para evitar que as vozes da call retornem aos participantes, inclusive quando o navegador ignorar as opções de exclusão. Uma mensagem explica como compartilhar o áudio de uma aba.
4. A prévia do transmissor contém **apenas vídeo**, sem retorno local. Abrir o próprio link no mesmo perfil também mantém a prévia sem som.
5. Quem assiste pode usar **Ativar áudio** / **Silenciar áudio**. Quando o navegador bloquear a reprodução com som, aparece **Ativar áudio da transmissão**. A preferência permanece durante reconexões.
6. Para transmitir **fora de uma call**, desmarque a opção antes de iniciar. Nesse caso, o áudio de tela/sistema pode ser compartilhado e **Ativar microfone** permite adicionar sua voz. Se entrar numa call depois, reinicie a transmissão com o modo de call marcado para não duplicar a voz.

O aplicativo exibe quando a tela foi compartilhada sem áudio. A captura varia conforme o navegador, o sistema e a superfície escolhida. Uma faixa de áudio geral já misturada não permite separar com segurança as vozes do Discord do som de um jogo; por isso o modo de call usa áudio de aba e bloqueia áudio de janela/tela inteira. Para jogo/aplicativo nativo com som isolado, seria necessária uma solução de captura ou roteamento de áudio por aplicativo. Abrir o próprio link em outra aba gera uma identidade de observador para esse acesso, preservando a sessão do transmissor.

## Conexões e capacidade

A transmissão usa uma conexão WebRTC por observador. A qualidade, resolução e FPS se adaptam por conexão, e o orçamento de upload é dividido entre os observadores. O padrão é até 8 espectadores e orçamento de 36 Mbps; configure `STREAM_UPLOAD_BUDGET_BPS` de acordo com o upload sustentado do transmissor. Essa configuração é um teto, não uma medição automática da velocidade. A capacidade também respeita a reserva mínima de vídeo, áudio e overhead.

Reconexões têm espera progressiva, detecção de sinalização sem resposta, nova tentativa quando a rede volta e preservação da audiência por 30 segundos de desconexão. Cada observador coordena sua recuperação. Pedidos e sessões têm identificadores que descartam mensagens antigas após trocar ou fechar uma live.

**TURN é necessário para alcançar redes onde a conexão direta é bloqueada**, como alguns CGNATs e firewalls. STUN sozinho não garante acesso entre quaisquer redes. Configure um serviço TURN real no ambiente do servidor; o aplicativo não fornece um servidor TURN embutido. Prefira endpoints UDP e TCP/TLS (por exemplo, porta 443) oferecidos pelo seu serviço, usando os endereços e credenciais dele.

- `TURN_URLS`: URLs TURN separadas por vírgula.
- `TURN_SECRET`: segredo compartilhado para emitir credenciais temporárias, com um servidor TURN compatível com esse mecanismo.
- `TURN_TTL_SECONDS`: duração das credenciais, padrão 3600 segundos.
- Como alternativa, `TURN_USERNAME`, `TURN_CREDENTIAL` e `ALLOW_STATIC_TURN_CREDENTIALS=true` habilitam credenciais estáticas.
- `FORCE_TURN_RELAY=true`: força relay quando TURN está configurado.
- `MAX_VIEWERS_PER_STREAM`: limite solicitado, padrão 8, máximo 50, também limitado pelo orçamento de upload.
- `ALLOWED_ORIGINS`: origens HTTPS permitidas, separadas por vírgula; sem configuração, a origem deve corresponder ao host.

`/health` informa `turnConfigured`, presença de endpoint TLS e contadores. Isso confirma a configuração, mas não testa se o serviço TURN está alcançável. Para audiências grandes, o modelo de uma conexão por observador exige mais upload e processamento; um SFU seria uma evolução de arquitetura.

## Validação

Com Node.js 20+ e Microsoft Edge instalado, execute `npm test`. É possível selecionar outro canal instalado com `PLAYWRIGHT_CHANNEL=chrome` ou instalar Edge com `npx playwright install msedge` no ambiente de testes.

Os testes verificam sinalização, limite de espectadores, mensagens atrasadas, reconexão, isolamento entre observadores, autoplay bloqueado, controles de áudio, recusa de microfone e cancelamento durante uma permissão pendente. Também verificam que áudio geral de monitor/janela é bloqueado em call, que áudio de aba chega ao observador e que a prévia não tem uma faixa de áudio. Usam navegadores reais com tela e microfone sintéticos: ICE, RTP, codificação, decodificação e reprodução são reais. Não substituem uma validação entre redes externas usando seu serviço TURN e seus dispositivos de áudio.

Referências: [captura de tela](https://developer.mozilla.org/en-US/docs/Web/API/MediaDevices/getDisplayMedia), [exclusão de áudio geral para evitar retorno em calls](https://developer.chrome.com/docs/web-platform/screen-sharing-controls#the_systemaudio_option), [reprodução e bloqueio de autoplay](https://developer.mozilla.org/en-US/docs/Web/API/HTMLMediaElement/play), [conexões WebRTC e TURN](https://webrtc.org/getting-started/peer-connections).
